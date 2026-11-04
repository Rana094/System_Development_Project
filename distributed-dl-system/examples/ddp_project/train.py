"""Train one classifier across CPU workers with sharded data and DDP gradients."""
import argparse
import csv
from datetime import timedelta
import hashlib
import json
import os
import random
from pathlib import Path
import time

# Include PyTorch import/setup in program time; keep timing outside checkpoint state.
PROGRAM_STARTED = time.perf_counter()

import torch
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel
from torch.utils.data import DataLoader, TensorDataset, DistributedSampler


def model_hash(model):
    digest = hashlib.sha256()
    for name,tensor in model.state_dict().items():
        digest.update(name.encode())
        digest.update(bytes(tensor.detach().cpu().contiguous().view(torch.uint8).flatten().tolist()))
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--epochs',type=int,default=40)
    parser.add_argument('--batch-delay',type=float,default=0.2)
    parser.add_argument('--global-batch-size',type=int,default=64)
    parser.add_argument('--log-every',type=int,default=1)
    parser.add_argument('--dropout',type=float,default=0)
    args = parser.parse_args()
    if args.epochs < 1 or args.batch_delay < 0 or args.log_every < 1 or not 0 <= args.dropout < 1:
        parser.error('Invalid training bounds')
    torch.set_num_threads(int(os.environ.get('OMP_NUM_THREADS','1')))
    torch.manual_seed(123)
    rank,world_size = int(os.environ.get('RANK','0')),int(os.environ.get('WORLD_SIZE','1'))
    distributed = world_size > 1
    if args.global_batch_size < world_size or args.global_batch_size % world_size:
        parser.error('Global batch size must be positive and divisible by worker count')
    if distributed:
        dist.init_process_group('gloo',timeout=timedelta(seconds=120))
    try:
        with open(os.environ['DL_DATASET_PATH'],newline='') as stream:
            rows = list(csv.DictReader(stream))
        inputs = torch.tensor([[float(row['x1']),float(row['x2'])] for row in rows])
        labels = torch.tensor([int(row['label']) for row in rows],dtype=torch.long)
        dataset = TensorDataset(inputs,labels)
        # Every rank receives a separate data shard. set_epoch changes the shared
        # shuffle each epoch while preserving equal collective step counts.
        sampler = DistributedSampler(dataset,num_replicas=world_size,rank=rank,seed=123)
        loader_generator = torch.Generator().manual_seed(900+rank)
        loader = DataLoader(dataset,batch_size=args.global_batch_size//world_size,sampler=sampler,
                            num_workers=0,generator=loader_generator)
        layers = [torch.nn.Linear(2,32),torch.nn.ReLU()]
        if args.dropout:
            layers.append(torch.nn.Dropout(args.dropout))
        layers += [torch.nn.Linear(32,16),torch.nn.ReLU(),torch.nn.Linear(16,2)]
        model = torch.nn.Sequential(*layers)
        if distributed:
            model = DistributedDataParallel(model)
        underlying = model.module if distributed else model
        optimizer = torch.optim.Adam(model.parameters(),lr=0.003)
        checkpoints = scheduler = restored = None
        initial_hash = model_hash(underlying)
        if distributed and os.environ.get('DL_RECOVERY_MODE')=='checkpoint':
            from dl_checkpoint import DistributedCheckpointManager
            scheduler = torch.optim.lr_scheduler.StepLR(optimizer,step_size=100,gamma=0.8)
            checkpoints = DistributedCheckpointManager()
            torch.manual_seed(123+rank)
            random.seed(123+rank)
            import numpy as np
            np.random.seed(123+rank)
            def restore_application(extra,progress):
                sampler_state = extra['sampler']
                if (sampler_state['rank'] != rank or sampler_state['world_size'] != world_size
                        or sampler_state['seed'] != 123):
                    raise ValueError('Saved sampler differs from this logical rank')
                epoch,batch = progress['next_epoch'],progress['next_batch']
                if (type(epoch) is not int or type(batch) is not int
                        or not 0 <= epoch <= args.epochs or not 0 <= batch < len(loader)
                        or (epoch == args.epochs and batch != 0)):
                    raise ValueError('Invalid saved data position')
                loader_generator.set_state(extra['loader_rng'])
                sampler.set_epoch(epoch)
            restored = checkpoints.load(model,optimizer,scheduler=scheduler,
                                        restore_extra=restore_application)
        steps = restored['global_step'] if restored else 0
        start_epoch = restored['progress']['next_epoch'] if restored else 0
        start_batch = restored['progress']['next_batch'] if restored else 0
        if steps != start_epoch*len(loader)+start_batch:
            raise ValueError('Saved step and input cursor disagree')
        sampler.set_epoch(0)
        first_shard = list(iter(sampler))
        print(f'DDP_START rank={rank} world_size={world_size} samples={len(sampler)}',flush=True)
        if distributed:
            dist.barrier()
        training_start = time.perf_counter()
        for epoch in range(start_epoch,args.epochs):
            sampler.set_epoch(epoch)
            saved_loader_rng = loader_generator.get_state()
            iterator = iter(loader)
            # A resumed mid-epoch iterator needs a base seed, but the saved
            # generator already advanced when that epoch originally began.
            if epoch == start_epoch and start_batch:
                loader_generator.set_state(saved_loader_rng)
            for batch_index,(batch_inputs,batch_labels) in enumerate(iterator):
                if epoch == start_epoch and batch_index < start_batch:
                    continue
                optimizer.zero_grad()
                loss = torch.nn.functional.cross_entropy(model(batch_inputs),batch_labels)
                loss.backward()  # DDP averages gradients across all workers here.
                optimizer.step()
                steps += 1
                if checkpoints:
                    scheduler.step()
                    next_epoch,next_batch = ((epoch+1,0) if batch_index+1==len(loader)
                                            else (epoch,batch_index+1))
                    checkpoints.save(model,optimizer,scheduler=scheduler,global_step=steps,
                        progress={'next_epoch':next_epoch,'next_batch':next_batch},
                        extra_state={'sampler':{'seed':123,'epoch':epoch,'rank':rank,'world_size':world_size},
                                     'loader_rng':loader_generator.get_state()})
                if args.batch_delay:
                    time.sleep(args.batch_delay)
            if (epoch+1) % args.log_every == 0 or epoch+1 == args.epochs:
                print(f'rank={rank} epoch={epoch+1} steps={steps} loss={loss.item():.6f}',flush=True)
        if distributed:
            dist.barrier()
        training_seconds = time.perf_counter() - training_start
        if checkpoints:
            checkpoints.save(model,optimizer,scheduler=scheduler,global_step=steps,
                progress={'next_epoch':args.epochs,'next_batch':0},
                extra_state={'sampler':{'seed':123,'epoch':args.epochs,'rank':rank,'world_size':world_size},
                             'loader_rng':loader_generator.get_state()},
                completed=True,force=True)

        # Verify that workers finished with identical parameters, rather than
        # merely running unrelated training programs on two machines.
        parameters = torch.cat([p.detach().flatten() for p in underlying.parameters()])
        peer_parameters = [torch.empty_like(parameters) for _ in range(world_size)]
        if distributed:
            dist.all_gather(peer_parameters,parameters)
        else:
            peer_parameters[0].copy_(parameters)
        synchronized = all(torch.equal(parameters,peer) for peer in peer_parameters)
        if not synchronized:
            raise ValueError('DDP parameters differ across ranks')
        model.eval()
        with torch.no_grad():
            logits = underlying(inputs)
            training_accuracy = (logits.argmax(1)==labels).float().mean().item()
            final_loss = torch.nn.functional.cross_entropy(logits,labels).item()
        output = Path(os.environ['DL_OUTPUT_DIR'])
        output.mkdir(parents=True,exist_ok=True)
        if rank == 0:
            torch.save({'model':underlying.state_dict(),'optimizer':optimizer.state_dict(),
                        'scheduler':scheduler.state_dict() if scheduler else None,'steps':steps},output/'model.pt')
        (output/'summary.json').write_text(json.dumps({
            'job_id':os.environ['DL_JOB_ID'],'rank':rank,'world_size':world_size,
            'task':'ddp-binary-classification','resumed_from':restored['global_step'] if restored else 0,
            'epochs':args.epochs,'training_steps':steps,'training_accuracy':training_accuracy,
            'final_loss':final_loss,
            'timing': {'scope': 'final_attempt', 'training_loop_seconds': training_seconds,
                       'training_program_seconds': time.perf_counter() - PROGRAM_STARTED,
                       'loop_includes': 'batches, synchronization, checkpoint saves, logging and configured batch delays',
                       'program_ends': 'after evaluation and model saving, before summary writing'},
            'training_seconds':training_seconds,'global_batch_size':args.global_batch_size,
            'local_batch_size':args.global_batch_size//world_size,
            'dataset_samples':len(dataset),'samples_processed_per_rank':len(sampler)*args.epochs,
            'first_epoch_indices':first_shard,'initial_model_sha256':initial_hash,
            'final_model_sha256':model_hash(underlying),'parameters_synchronized':synchronized,
            'checkpoint_enabled':checkpoints is not None,
        },indent=2))
        if distributed:
            dist.barrier()  # All ranks publish results before any rank exits.
        print(f'DDP_COMPLETE rank={rank} synchronized={synchronized}',flush=True)
    finally:
        if distributed:
            dist.destroy_process_group()


if __name__ == '__main__':
    main()

"""Versioned deterministic CSV inputs for the existing example trainers."""
import hashlib
import os
from pathlib import Path
import secrets

KINDS=('regression','cnn','ddp')
GENERATOR='sha256-counter-csv-v1'


def digest(path):
    result=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024),b''):result.update(block)
    return result.hexdigest()


def generate(directory,kind,rows,seed,*,max_bytes):
    if kind not in KINDS or type(rows) is not int or not 1<=rows<=1000000:
        raise ValueError('Invalid dataset kind/row count')
    if type(seed) is not int or not 0<=seed<=2**32-1 or type(max_bytes) is not int or max_bytes<1:
        raise ValueError('Invalid dataset seed/byte limit')
    directory=Path(directory);target=directory/f'{kind}-{rows}.csv'
    part=directory/('.dataset-'+secrets.token_hex(16)+'.part')
    fd=os.open(part,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    hashed=hashlib.sha256();size=0
    try:
        with os.fdopen(fd,'wb') as stream:
            def write(text):
                nonlocal size
                raw=text.encode('ascii')
                if size+len(raw)>max_bytes:raise ValueError('Dataset exceeds configured byte limit')
                stream.write(raw);hashed.update(raw);size+=len(raw)
            header='x,y' if kind=='regression' else ','.join([*(f'pixel{i}' for i in range(64)),'label']) if kind=='cnn' else 'x1,x2,label'
            write(header+'\n')
            for index in range(rows):
                raw=hashlib.sha256(f'{GENERATOR}:{seed}:{index}'.encode('ascii')).digest()
                if kind=='regression':
                    x=int.from_bytes(raw[:2],'big')%2001-1000
                    write(f'{x/1000:.3f},{(2*x+1000)/1000:.3f}\n')
                elif kind=='ddp':
                    x,y=(int.from_bytes(raw[i:i+2],'big')%2001-1000 for i in (0,2))
                    write(f'{x/1000:.3f},{y/1000:.3f},{int(x+y>=0)}\n')
                else:
                    label=index%3
                    pixels=[f'{((raw[i%32]+31*i+label*97)%1000)/1000:.3f}' for i in range(64)]
                    write(','.join([*pixels,str(label)])+'\n')
            stream.flush();os.fsync(stream.fileno())
        # Hard link gives no-replace publication of a new synthetic dataset;
        # this is not a writable workspace link to any persistent cache.
        os.link(part,target,follow_symlinks=False);target.chmod(0o444)
        return dict(schema_version=1,dataset_id=f'eval-{kind}-s{seed}-r{rows}',version=1,
                    generator=GENERATOR,seed=seed,rows=rows,kind=kind,path=target.name,
                    sha256=hashed.hexdigest(),size_bytes=size)
    finally:part.unlink(missing_ok=True)

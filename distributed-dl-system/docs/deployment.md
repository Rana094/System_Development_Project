# Step 12: installation and full-system inspection

The same controller and worker code runs on the three Pis and on Linux lab PCs.
Worker installation, registration, startup services and environment preparation
are now repeatable. The scheduler reads `config/nodes.json`; adding a machine
requires no scheduler code change.

## 1. Inspect the installed Pi system

Run these commands from the project directory:

```bash
cd "/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system"
python3 tools/cluster_setup.py check
systemctl --user status dl-dispatcher dl-student-portal --no-pager
loginctl show-user "$USER" -p Linger
```

For each reachable Pi, expect its worker ID, `aarch64`, Python version, clock
difference below two seconds, and IDLE when no training is running. Powered-off
workers will be unavailable; choose a job size that fits the online cluster. Both controller services
should be `active (running)` and `Linger=yes`. Linger starts the user service
manager at boot even before a desktop login. It does not prevent laptop sleep;
keep the controller powered on and awake during training.

Each Pi has an enabled `dl-node-agent` system service:

```bash
ssh pi01 'systemctl is-enabled dl-node-agent; systemctl is-active dl-node-agent'
ssh pi02 'systemctl is-enabled dl-node-agent; systemctl is-active dl-node-agent'
ssh pi03 'systemctl is-enabled dl-node-agent; systemctl is-active dl-node-agent'
```

Controller logs now go to the journal:

```bash
journalctl --user -u dl-dispatcher -f
# In another terminal:
journalctl --user -u dl-student-portal -f
```

Do not also start `controller/dispatcher.py` or `controller/student_portal.py`
manually while their services are running. Their locks prevent duplicate owners.

## 2. Submit real training and observe scheduling

Create your own password if you have not already set one:

```bash
.controller-venv/bin/python controller/student_portal.py --create-user student01
```

Open **http://127.0.0.1:8080** and sign in. Follow
[the browser instructions](student-interface.md) for exact form controls.

1. Choose **Regression**, submit it and select its job.
2. Observe environment preparation, QUEUED, the assigned Pi, RUNNING, live logs,
   checkpoint step, and finally COMPLETED. Download its results.
3. Submit **Binary classification** and **CNN** close together. With two free
   compatible Pis, these independent jobs can run on different Pis simultaneously.
4. Submit the **DDP** example. Inspect its two ranks and two reserved Pis. This is
   one training job using two workers together; it needs both slots available.
5. Submit another job, try Hold/Release while queued, and Cancel while running.
   Check the recorded status and attempts rather than only watching terminal text.

You can inspect any browser job in a terminal:

```bash
JOB_ID='paste-the-job-id-from-the-page'
.controller-venv/bin/python controller/job_status.py "$JOB_ID"
```

Training sources are `examples/checkpoint_project/train.py`,
`examples/classification_project/train.py` and `examples/ddp_project/train.py`.
The portal packages those real programs using the same submission path used for
student ZIP uploads. Built-in examples prepare a native `pytorch-cpu` environment
on each configured worker. The first preparation on a new PC may download PyTorch;
subsequent jobs with matching source/dependency and runtime/profile identities
reuse the verified environment cache. Changing source can require another build.

## 3. Inspect whole-Pi reboot recovery

Use the longer regression settings from
[the checkpoint integration instructions](checkpoint-integration.md) so there is
time to observe progress. First run an uninterrupted baseline. Submit an identical
second job, wait for a **nonzero central checkpoint**, and note its assigned Pi.
Reboot only that assigned worker, for example:

```bash
ssh pi03 'sudo -n reboot'
```

A closed SSH connection during reboot is expected. The dispatcher should record
the failed attempt, start a replacement attempt from the central checkpoint, and
finish the job. Observe the resumed step and recovery events in the portal.
Then compare the two job IDs:

```bash
BASELINE_ID='paste-baseline-job-id'
RECOVERY_ID='paste-recovered-job-id'
.controller-venv/bin/python tools/comparisons/compare_regression_jobs.py "$BASELINE_ID" "$RECOVERY_ID"
```

Expect `pass: true`, matching model/optimizer/scheduler state, MSE and predictions,
and a nonzero `resumed_from` for the recovered run. The controlled regression
example is deterministic; arbitrary student models are not guaranteed to be
bitwise deterministic unless their training code and checkpoint integration are.

For DDP, follow [the group recovery instructions](distributed-training.md#step-9-coordinated-ddp-checkpoint-recovery).
Reboot one assigned rank only after a committed group checkpoint exists. The system
must stop/recreate the whole group, restore every rank and then continue. A
single-rank resume while the other rank continues is not the expected behavior.

After the Pi returns, `cluster_setup.py check` should show it online again without
manually starting its worker service. This checks boot-time worker startup as well
as recovery. [Execution ownership](execution-ownership.md) also documents the
separate network-partition test and lease expiration behavior.

## 4. Repeat installation or update worker code

Let running jobs finish or cancel them and wait for cancellation to complete.
Stop the controller services before worker maintenance:

```bash
systemctl --user stop dl-student-portal dl-dispatcher
python3 tools/cluster_setup.py install-workers --nodes pi01 pi02 pi03
python3 tools/cluster_setup.py check
systemctl --user start dl-dispatcher dl-student-portal
```

The installer refuses active database jobs/reservations, a running dispatcher or
portal, and busy reachable workers. It stages source, checks SHA-256 digests,
compiles Python and validates the unit before replacing worker code. If activation
fails, it restores the previous managed source and unit. Service dependencies that
pip installed remain installed; this is not a complete virtual-environment rollback.

Only `agent/`, the training Python files and the two required configuration files
are transferred. Existing `.venv`, `worker-storage/`, checkpoint files, lease
ownership generations and logs are retained. One previous source snapshot is kept
at `worker-storage/deployment-previous` on each worker. Last successful deployment
checks and source hashes are in `logs/deployment-verification.json` on the controller.
No friend's source directory is edited.

## 5. Add a lab PC

Start with two Linux PCs before increasing to five and then thirty. Each needs:

- A stable LAN IPv4 address, SSH server, synchronized clock and enough disk/RAM.
- Python 3.11 or 3.12 with its matching `venv`/pip support. Use a common Python and
  PyTorch version across a DDP group. Do not copy ARM virtual environments to x86.
- An operator SSH account with key-based login and noninteractive sudo for service
  installation. Establish this with the lab administrator; this tool does not
  change passwords, SSH trust or sudo permissions.
- Connectivity from controller to worker API (default TCP 5000), controller SSH
  access, and communication between workers for Gloo rendezvous/data traffic.
  Restrict these connections to the trusted cluster network.

For example, configure this alias in the controller operator's `~/.ssh/config`,
using your actual lab address and account:

```sshconfig
Host lab01
    HostName 192.168.10.201
    User labworker
    IdentityFile ~/.ssh/id_ed25519
```

Verify the host key and credentials yourself once, then install/register it:

```bash
ssh lab01 'python3 --version; sudo -n true'
systemctl --user stop dl-student-portal dl-dispatcher
python3 tools/cluster_setup.py add-worker \
  --id lab01 --address 192.168.10.201 \
  --root distributed-dl-system --python python3
python3 tools/cluster_setup.py check
systemctl --user start dl-dispatcher dl-student-portal
```

`--id` is both the logical scheduler ID and SSH alias. `--address` is the LAN API
address. `--root` is relative to the remote SSH home unless absolute. `--python`
selects the base interpreter **only when creating a missing `.venv`**; it does not
replace an existing environment. Use an explicit interpreter path when needed.
`--port` defaults to 5000. Normally the network interface is detected from the
advertised address; `--interface eno1` overrides it.

Registration happens only after installation, matching worker identity, ownership
protocol, resource response and clock checks succeed. Duplicate IDs or endpoints
are rejected. A repeated update uses `install-workers --nodes lab01`, not
`add-worker`. Submit a small single job and then a two-PC DDP job before expanding.
No changes to `scheduler.py` are required.

## 6. Install the controller on another Linux machine

Copy the project source and configure its worker SSH aliases. Create native
virtual environments on the destination; do not copy `.controller-venv` from
another machine. Keep **one active controller** for this cluster. On the new machine:

```bash
python3 tools/cluster_setup.py install-controller
.controller-venv/bin/python controller/student_portal.py --create-user student01
python3 tools/cluster_setup.py check
```

This creates `.controller-venv` if missing, installs lightweight service packages,
installs/enables user systemd units, enables lingering, and starts the dispatcher
and portal. Existing users, database and storage are preserved. If local policy
requires administrator privileges for lingering, an administrator must enable it:
`sudo loginctl enable-linger "$USER"`. Rerun the installer after that.

The lightweight installer does not install PyTorch on the controller. Normal
student submission and SDK checkpoint transfer do not need it there. To run the
tensor comparison tools, the full test suite or legacy built-in PyTorch job
validation on a fresh controller, install the matching CPU PyTorch and NumPy:

```bash
.controller-venv/bin/python -m pip install torch --index-url https://download.pytorch.org/whl/cpu
.controller-venv/bin/python -m pip install numpy
```

For a controlled LAN pilot, stop the controller services, rerun
`install-controller --host 0.0.0.0` and access
`http://CONTROLLER_LAN_IP:8080`. The default is laptop-local access. The installed
HTTP services use [Waitress](https://docs.pylonsproject.org/projects/waitress/en/latest/usage.html).

To migrate existing history, stop both old controller services and finish active
jobs first. Preserve `controller/jobs.db` together with `storage/`, `checkpoints/`,
`config/` and necessary logs. SQLite WAL files, if present, belong with the stopped
database. Stored job paths are absolute: retain the same project/storage path on
the new controller, or perform a deliberate path migration before resuming it.
Do not start a second controller against the same workers with an empty ownership
database. Test a fresh small submission and recovery before retiring the old host.

This completes repeatable deployment for the **trusted CPU cluster prototype**.
Thirty-PC load testing, GPU/NCCL support, HTTPS and isolation of untrusted student
code are separate lab-rollout work; they have not been demonstrated on these Pis.
The portal's per-student login protects job access, not the worker operating system.

## Checks

```bash
.controller-venv/bin/python tests/test_cluster_setup.py -v
```

The existing test modules use temporary controller copies and some change process
state during import. Run each module as its own process, rather than combining
them with unittest discovery:

```bash
for test_file in tests/test_*.py; do
  .controller-venv/bin/python "$test_file" || break
done
```

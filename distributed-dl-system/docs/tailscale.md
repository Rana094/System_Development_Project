# Remote access over Tailscale

Configured on the current controller laptop on 2026-11-04.

Connect the browser device to the same Tailscale network and open
**http://100.73.90.91:8080/**. Sign in with the existing website account.
Local access at http://127.0.0.1:8080/ still works.

The laptop hosts the controller and website: it must remain awake, online,
and connected to Tailscale. The Pis need internet and Tailscale, but the laptop
does not need to share their local network. This is private tailnet access;
HTTP traffic travels inside the encrypted Tailscale tunnel.

## Installed configuration

- `config/nodes.json` uses pi01 `100.101.90.46`, pi02 `100.91.135.18`,
  and pi03 `100.64.202.71` for worker API calls and DDP rendezvous.
- The controller user's `~/.ssh/config` aliases use the same addresses for
  environment preparation, uploads, and artifact downloads. `HostKeyAlias`
  preserves each previously trusted LAN host identity. The prior configuration
  is backed up at `~/.ssh/config.before-dl-tailscale-20261104`.
- `~/.config/systemd/user/dl-portal-tailscale.socket` listens only on
  `100.73.90.91:8080`, with `FreeBind=yes`, and is enabled at user-service startup.
- `dl-portal-tailscale.service` uses
  `/usr/lib/systemd/systemd-socket-proxyd 127.0.0.1:8080` to forward connections
  to the existing local website. No public listener or router forwarding is needed.

Check the installation:

```bash
tailscale status
systemctl --user is-active dl-student-portal dl-dispatcher dl-portal-tailscale.socket
python3 tools/cluster_setup.py check
curl -I http://100.73.90.91:8080/
```

Worker health/identity, SSH to all three workers, and an HTTP GET from pi03
to the website were checked. No training job was submitted for this network change.

## Optional HTTPS

Tailscale Serve was not configured: HTTPS/Serve requires enabling the feature
in the Tailscale account, and this laptop requires administrator permission to
write Serve configuration. After enabling it, an administrator can run:

```bash
sudo tailscale serve --bg http://127.0.0.1:8080
tailscale serve status
```

Use the HTTPS URL printed by that command. The private HTTP endpoint can then
be removed with `systemctl --user disable --now dl-portal-tailscale.socket`
followed by `systemctl --user stop dl-portal-tailscale.service`.

If the laptop is removed and re-enrolled in Tailscale with a different IP,
update the socket's `ListenStream` address and restart the socket. A replacement
worker address must be updated in both the node inventory and SSH configuration.

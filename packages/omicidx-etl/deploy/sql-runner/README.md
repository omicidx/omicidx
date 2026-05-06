# SQL Runner - systemd Deployment

The Daily SQL Runner runs on the local Rocky Linux server via systemd, replacing the GitHub Actions scheduled workflow. A lightweight GH Actions workflow receives status reports for public visibility.

## Prerequisites

- `uv` installed and on PATH
- `gh` CLI installed and authenticated (needs `workflow` scope)
- This repo cloned to `/home/davsean/Documents/git/omicidx-etl`

## Setup

1. Create the config directory and environment file:

```bash
mkdir -p ~/.config/omicidx
cp deploy/sql-runner/env.example ~/.config/omicidx/env
# Edit ~/.config/omicidx/env and fill in all values
chmod 600 ~/.config/omicidx/env
```

2. Make the wrapper script executable:

```bash
chmod +x deploy/sql-runner/run-and-report.sh
```

3. Install the systemd units (user-level):

```bash
mkdir -p ~/.config/systemd/user
ln -sf "$(pwd)/deploy/sql-runner/omicidx-sql-runner.service" ~/.config/systemd/user/
ln -sf "$(pwd)/deploy/sql-runner/omicidx-sql-runner.timer" ~/.config/systemd/user/
systemctl --user daemon-reload
```

4. Enable and start the timer:

```bash
systemctl --user enable --now omicidx-sql-runner.timer
```

5. Enable lingering so user services run without an active login session:

```bash
sudo loginctl enable-linger davsean
```

## Usage

Check timer status:

```bash
systemctl --user status omicidx-sql-runner.timer
systemctl --user list-timers
```

Trigger a manual run:

```bash
systemctl --user start omicidx-sql-runner
```

View logs:

```bash
journalctl --user -u omicidx-sql-runner
journalctl --user -u omicidx-sql-runner -f  # follow
```

Check last run result:

```bash
systemctl --user status omicidx-sql-runner.service
```

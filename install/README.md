# Setup irodsfs-pool systemd service

Copy the iRODS FS Pool Server binary `bin/irodsfs-pool` to `/usr/bin/`.

Copy the systemd service `irodsfs-pool.service` to `/usr/lib/systemd/system/`.

Create a service user `irodsfs-pool`.
```bash
sudo adduser -r -d /dev/null -s /sbin/nologin irodsfs-pool
```

Copy the iRODS FS Pool Server configuration `config.yaml` to `/etc/irodsfs-pool/`.
Be sure that this file must be only accessible by the `irodsfs-pool` user.

Start the service.
```bash
sudo service irodsfs-pool start
```
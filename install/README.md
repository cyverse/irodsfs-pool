# Setup irodsfs-pool systemd service

Use `Makefile` in install package. 

```bash
sudo make install
```

Enable the service.
```bash
sudo systemctl enable irodsfs-pool.service
```

Start the service.
```bash
sudo service irodsfs-pool start
```

Check the service status.
```bash
sudo service irodsfs-pool status
```
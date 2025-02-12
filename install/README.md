# Setup irodsfs-pool systemd service

Use `Makefile` in install package. 

If you are installing on ubuntu,
```bash
sudo make install_ubuntu
```

If you are installing on CentOS,
```bash
sudo make install_centos
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
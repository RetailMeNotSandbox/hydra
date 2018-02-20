# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

$script = <<SCRIPT

    sudo chmod +x /vagrant/ansible/ansible-bootstrap.sh
    /vagrant/ansible/ansible-bootstrap.sh

    if [ ! -d "/etc/ansible" ]; then
      sudo mkdir /etc/ansible
    fi

    sudo cp /vagrant/ansible/local /etc/ansible/hosts
    sudo chmod -x /etc/ansible/hosts  # ansible will try to execute it if it's marked executable; vagrant's shared folders on windows won't persist this change, hence the copy
    # PYTHONUNBUFFERED=1 from https://groups.google.com/d/msg/ansible-project/aXutTmXAbR0/2vq4GBD1I8MJ
    sudo PYTHONUNBUFFERED=1 ansible-playbook /vagrant/ansible/vagrant-dev.yml --extra-vars "db_password=vagrant"

SCRIPT

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = "ubuntu/trusty64"
  # forward the default web app port
  config.vm.network "forwarded_port", guest: 9000, host: 9000
  # forward the default postgres port
  config.vm.network "forwarded_port", guest: 5432, host: 5432
  config.vm.synced_folder ".", "/vagrant", type: "rsync", rsync__exclude: [".git/", "target/"], rsync__auto: true

  config.vm.provider "virtualbox" do |vb|
    vb.customize ["modifyvm", :id, "--memory", "2048"]
  end

  config.vm.provision "shell", inline: $script

end

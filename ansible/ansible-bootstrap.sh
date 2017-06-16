#!/bin/bash
if !(hash ansible-playbook 2>/dev/null); then 
  sudo apt-get update
  sudo apt-get install build-essential python-dev python-pip libffi-dev libssl-dev -y
  sudo pip install pip==7.1.2
  sudo pip install ansible==1.9.3
fi

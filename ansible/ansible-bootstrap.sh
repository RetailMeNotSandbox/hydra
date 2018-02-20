#!/bin/bash
if !(hash ansible-playbook 2>/dev/null); then
  sudo apt-get update
  sudo apt-get install build-essential libssl-dev libffi-dev python-dev python-pip -y
  sudo pip install pip==9.0.1
  sudo pip install ansible==1.9.3
fi

# Naive Cassandra

> course project for *Cloud Data Management (1)*

### 1. Current progress

* Finished:
  *  Gossip protocol
* In progress:
  * Partition

### 2. Environment

* python 3.5+
* apscheduler (`pip install apscheduler`)

### 3. Usage

`python main.py -c <config_file_path>`

Sample config files can be found at `config/*.ini`

### 4. Code structure

`cassandra/conn`: P2P communication architecture

`cassandra/gossip`: Gossip protocol

`cassandra/util`: utility functions and classes

##### 4.1 P2P architecture

##### 4.2 Gossip protocol
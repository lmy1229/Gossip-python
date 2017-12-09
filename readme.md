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

* **Gossip timer task**

  > Runs every second. During each of these runs the node initiates gossip exchange according to following rules:
  >
  > 1. Gossip to random live endpoint (if any).
  > 2. Gossip to random unreachable endpoint with certain probability depending on number of unreachable and live nodes.
  > 3. If the node gossiped to at (1) was not seed, or the number of live nodes is less than number of seeds, gossip to random seed with certain probability depending on number of unreachable, seed and live nodes.
  >
  > These rules were developed to ensure that if the network is up, all nodes will eventually know about all other nodes. (Clearly, if each node only contacts one seed and then gossips only to random nodes it knows about, you can have partitions when there are multiple seeds -- each seed will only know about a subset of the nodes in the cluster. Step 3 avoids this and more subtle problems.)
  >
  > This way a node initiates gossip exchange with one to three nodes every round (or zero if it is alone in the cluster)
  >
  > (from https://wiki.apache.org/cassandra/ArchitectureGossip)

* **Data structures**

  * **VersionedValue**. Consists of value and version number
  * **HeartBeatState**. Consists of generation and version number. Generation stays the same when server is running and grows every time the node is started. Used for distinguishing state information before and after a node restart. Version number is shared with application states and guarantees ordering. Each node has one HeartBeatState associated with it.

  * **ApplicationState**. Consists of identifiers of states within cassandra  such as 'STATUS', 'LOAD'.
  * **EndPointState**. Includes all ApplicationStates and HeartBeatState for certain endpoint (node). EndPointState can include only one of each type of ApplicationState, so if EndPointState already includes, say, load information, new load information will overwrite the old one. ApplicationState version number guarantees that old value will not overwrite new one.

  * **endPointStateMap**. Internal structure in Gossiper that has EndPointState for all nodes (including itself) that it has heard about.

* **Gossip Exchange**

  * **GossipDigestSynMessage**. Node starting gossip exchange sends GossipDigestSynMessage, which includes a list of gossip digests. A single gossip digest consists of endpoint address, generation number and maximum version in EndPointState that has been seen for the endpoint. For example, the following message is a serialized GossipDigestSynMessage:

    ```
    b'{"type": "GossipDigestSyn", "params": [["127.0.0.1:7002", 1512784972, 1], ["127.0.0.1:7001", 1512784956.437975, 5]]}'
    ```

  * **GossipDigestAckMessage**. A node receiving GossipDigestSynMessage will examine it and reply with GossipDigestAckMessage, which includes _two_ parts: gossip digest list to request and endpoint state list to send. From the gossip digest list arriving in GossipDigestSynMessage we will know for each endpoint whether the sending node has newer or older information than we do. For example, the following message is a serialized GossipDigestAckMessage(epStateMap is empty because we do not record any state information yet):

    ```
    b'{"type": "GossipDigestAck", "params": {"gDigests": [["127.0.0.1:7001", 1512784956.437975, 0]], "epStateMap": ""}}'
    ```

  * **GossipDigestAck2Message**. Containing any information that remote endpoint requested or needs to be updated in gossip digest list of GossipDigestAckMessage. For example:

    ```
    b'{"type": "GossipDigestAck2", "params": "127.0.0.1:7001-[HeartBeat, generation 1512784956, version 5]"}'
    ```


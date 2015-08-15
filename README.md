Openbmp Basic File Consumer
===========================
A basic file consumer of OpenBMP ***parsed*** and ***RAW BMP*** Kafka streams.  You can use this file consumer in the following ways:

* Working example to develop your own consumer that works with either parsed or RAW BMP binary messages
* Record BMP feeds (identical as they are sent by the router) so they can be replayed to other BMP parsers/receivers
* Log parsed BMP and BGP messages in plain flat files

> See [Consumer Developer Integration](http://www.openbmp.org/#!docs/CONSUMER_DEVELOPER_INTEGRATION.md) for more details on the OpenBMP Kafka feeds and message syntax.

### Parsed Log Format
The current format of the log messages are an attempt to be human readable.  If you plan to use **grep**, **awk** or other tools to filter/interact with the flat files, then you will want to update **src/bin/openbmp-file-consumer** source file to change the logging format.  You can easily change the format to whatever you want.  For example, you can make it PIPE delimited, you can filter what's logged, etc.


Installation
------------
You can either run the code within the **git** directory or you can install it in your python path. 

> If you are going to run it within the **git** directory, see running instructions.  

### Install Dependancies:
    
    sudo apt-get install libsnappy-dev
    sudo pip install python-snappy
    sudo pip install kafka-python

> You might have to install '**pip**' if you don't already have that. You can do so using ```apt-get install python-pip python-dev```


*See [Kafka-python Install Instructions](http://kafka-python.readthedocs.org/en/latest/install.html) for more details.*

### Install openbmp-file-consumer:

    git clone https://github.com/OpenBMP/openbmp-file-consumer.git
    cd openbmp-file-consumer
    sudo python setup.py install


Running
-------
If you install the python code, then you should be able to run from a terminal

    openbmp-file-consumer -b <host e.g. kafka.openbmp.org> <base path for parsed message e.g. /var/openbmp>
    
If you are running from within the **git** directory, you can run it as follows:

    PYTHONPATH=./src/site-packages python src/bin/openbmp-file-consumer -b <host e.g. kafka.openbmp.org> <base path for parsed message e.g. /var/openbmp>

    
#### Usage
```
Usage: openbmp-file-consumer [OPTIONS] <directory>

<directory> is the directory where you want to store the data.
            Directory should already exist (e.g. /var/openbmp/files)

OPTIONS:
  -h, --help                  Print this help menu
  -b, --servers               Kafka bootstrap servers in the format of 'host:port[,...]'
  --disable-bgpls             Disable BGP link-state logging
  --disable-base_attr         Disable base attribute logging
  --disable-unicast_prefix    Disable unicast prefix logging
  --enable-bmp                Enable BMP RAW feed logging
  -i, --interval              Heartbeat interval in seconds for collectors (Default is 14400/4 hours)
```

> ##### NOTE
> To enable BMP RAW binary stream logging use the **--enable-bmp** option.

#### Example

```
tievens$ PYTHONPATH=./src/site-packages python src/bin/openbmp-file-consumer -b bmp-dev.openbmp.org -i 3 /var/tmp/openbmp
Connecting to ['bmp-dev.openbmp.org'] ... takes a minute to load offsets and topics, please wait
Connected, now consuming

tievens$ tail -f /var/tmp/openbmp/COLLECTOR_e086aa137fa19f67d27b39d0eca18610/ROUTER_x.x.x.x/PEER_wa1.level3.net/unicast_prefixes.txt 
 
2015-08-07 17:01:30.439892  Prefix: 63.77.246.0/24                           Origin AS: 705       
           AS Count: 3      NH: 4.68.1.197       LP: 0   MED: 0        Origin: igp          Aggregator:   ClusterList:  Originator Id: 
               Path:  3356 701 705 
        Communities: 3356:3 3356:86 3356:575 3356:666 3356:2050
 
2015-08-07 17:01:29.556874  Prefix: 41.231.128.0/19                          Origin AS: 5438      
           AS Count: 4      NH: 4.68.1.197       LP: 0   MED: 0        Origin: igp          Aggregator:   ClusterList:  Originator Id: 
               Path:  3356 6762 2609 5438 
        Communities: 3356:3 3356:22 3356:86 3356:575 3356:666 3356:2039 6762:1 6762:92 6762:13950
        
2015-08-07 17:02:07.260264  Prefix: 124.217.140.0/24                         Origin AS: 10118     
           AS Count: 6      NH: 4.68.1.197       LP: 0   MED: 0        Origin: igp          Aggregator: 64519 172.20.88.168  ClusterList:  Originator Id: 
               Path:  3356 1273 9381 10118 10118 10118 
        Communities: 1273:13344 1273:39760 3356:3 3356:22 3356:100 3356:123 3356:575 3356:2003 65003:0

```

Directory Structure
-------------------
Files are stored hierarchical as depicted below.  Files are rotated automatically after a size of 300MB. The max rotated file count is 20.  You can adjust these settings if you wish by updating the src/bin/openbmp-file-consumer **initLogger**() function. 

    Root/base directory
        |
        |---- FILE: collectors.txt                    # Collector messages
        |---- DIR: <collector hash id>
            |
            |---- FILE: routers.txt                   # Router messages
            |---- DIR: <router name and ip>
                |
                |---- FILE: bmp_feed.raw              # RAW BMP feed messages (stream)
                |---- FILE: peers.txt                 # Peer messages
                |---- DIR: <peer name and ip>
                    |
                    |---- FILE: bmp_stats.txt         # BMP statistic messages
                    |---- FILE: base_attributes.txt   # Base Attribute messages
                    |---- FILE: unicast_prefixes.txt  # Unicast Prefix messages
                    |---- FILE: ls_nodes.txt          # Link-state Node messages
                    |---- FILE: ls_links.txt          # Link-state Link messages
                    |---- FILE: ls_prefixs.txt        # Link-state Prefix messages
                    


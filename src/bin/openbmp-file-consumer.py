#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""OpenBMP basic file consumer

  Copyright (c) 2013-2015 Cisco Systems, Inc. and others.  All rights reserved.
  This program and the accompanying materials are made available under the
  terms of the Eclipse Public License v1.0 which accompanies this distribution,
  and is available at http://www.eclipse.org/legal/epl-v10.html

  .. moduleauthor:: Tim Evens <tievens@cisco.com>
"""
import time
import datetime
import calendar
import getopt
import sys
import os
import yaml
import socket
import logging
import logging.handlers
import kafka
import traceback

from openbmp.RawTimedRotatingFileHandler import RawTimedRotatingFileHandler

from openbmp.api.parsed.message import Message
from openbmp.api.parsed.message import Router
from openbmp.api.parsed.message import Collector
from openbmp.api.parsed.message import Peer

"""
from openbmp.parsed.headers import headers as parsed_headers
from openbmp.parsed.collector import collector
from openbmp.parsed.router import router
from openbmp.parsed.peer import peer
from openbmp.parsed.base_attribute import base_attribute
from openbmp.parsed.unicast_prefix import unicast_prefix
from openbmp.parsed.bmp_stat import bmp_stat
from openbmp.parsed.ls_node import ls_node
from openbmp.parsed.ls_link import ls_link
from openbmp.parsed.ls_prefix import ls_prefix
"""



# Collector logger hash to keep track of file loggers for collector object/messages
#     Key = <collector hash>
#     Value = Logger
COLLECTOR_LOGGERS = {}

# Collector admin ID lookup HASH
#    Key = <collector hash>, Value = (admin ID, lastTS)
COLLECTOR_ADMIN_ID = {}

# Hashes to track Loggers
#     Key = <collector hash>, Value = Dictionary (key = router hash id, value = logger)
ROUTER_LOGGERS = {}
PEER_LOGGERS = {}
BMP_STAT_LOGGERS = {}
BASE_ATTR_LOGGERS = {}
UNICAST_PREFIX_LOGGERS = {}
LS_NODE_LOGGERS = {}
LS_LINK_LOGGERS = {}
LS_PREFIX_LOGGERS = {}

# BMP RAW hash to keep track of opened files
#    Key = <collector hash>, Value = Dictionary (key = router hash_id, value = logger)
BMP_RAW_LOGGERS = {}

# Configuration
CONFIG = {}


def processCollectorMsg(msg, fdir):
    """ Process collector message

    :param msg:      Message object
    :param fdir:        Directory where to store the parsed files
    """
    obj = Collector(msg)

    c_hash = msg.getCollector_hash_id()
    map_list = obj.getRowMap()

    newCollector = False

    # Create logger if new
    if c_hash not in COLLECTOR_LOGGERS:
        filepath = fdir

        try:
            os.makedirs(filepath)
        except:
            pass

        COLLECTOR_LOGGERS[c_hash] = initLogger('openbmp.parsed.collector.' + c_hash,
                                               os.path.join(filepath, 'collectors.txt'))
        newCollector = True

    # Log messages
    for row in map_list:
        if len(row):
            try:
                # Save the collector admin ID to hash id
                cur_ts = calendar.timegm(datetime.datetime.strptime(row['timestamp'], "%Y-%m-%d %H:%M:%S.%f").timetuple())
                COLLECTOR_ADMIN_ID[c_hash] = (row['admin_id'], cur_ts)

                if row['action'] == 'heartbeat' and not newCollector:
                    continue

                COLLECTOR_LOGGERS[c_hash].info("%-27s Action: %-9s Admin Id: %s Hash Id: %s\n"
                                               "    Connected Routers: %s", row['timestamp'],
                                               row['action'], row['admin_id'], row['hash'], row['routers'])

                # clean up
                if row['action'] == 'stopped':
                    try:
                        del COLLECTOR_ADMIN_ID[c_hash]
                        del COLLECTOR_LOGGERS[c_hash]
                        del ROUTER_LOGGERS[c_hash]
                        del PEER_LOGGERS[c_hash]

                        if c_hash in BMP_STAT_LOGGERS:
                            del BMP_STAT_LOGGERS[c_hash]

                        if c_hash in UNICAST_PREFIX_LOGGERS:
                            del UNICAST_PREFIX_LOGGERS[c_hash]

                        if c_hash in BASE_ATTR_LOGGERS:
                            del BASE_ATTR_LOGGERS[c_hash]

                        if c_hash in LS_NODE_LOGGERS:
                            del LS_NODE_LOGGERS[c_hash]

                        if c_hash in LS_LINK_LOGGERS:
                            del LS_LINK_LOGGERS[c_hash]

                        if c_hash in LS_PREFIX_LOGGERS:
                            del LS_PREFIX_LOGGERS[c_hash]

                        for r in BMP_RAW_LOGGERS[c_hash]:
                            for f in BMP_RAW_LOGGERS[c_hash][r]:
                                f.close()

                        if c_hash in BMP_RAW_LOGGERS:
                            del BMP_RAW_LOGGERS[c_hash]
                    except:
                        pass

            except NameError as e:
                print e
                print "row = [%d] (%r)" % (len(row), row)

            except:
                pass


def processRouterMsg(msg, fdir):
    """ Process Router message

    :param msg:      Message object
    :param fdir:        Directory where to store the parsed files
    """
    obj = Router()

    c_hash = msg.getCollector_hash_id()
    map_list = obj.getRowMap()

    # Log messages
    for row in map_list:
        if len(row):
            try:
                # Create logger
                if c_hash not in ROUTER_LOGGERS or row['hash'] not in ROUTER_LOGGERS[c_hash]:
                    filepath = os.path.join(fdir, 'COLLECTOR_' + c_hash)

                    try:
                        os.makedirs(filepath)
                    except:
                        pass

                    if c_hash not in ROUTER_LOGGERS:
                        ROUTER_LOGGERS[c_hash] = {}

                    ROUTER_LOGGERS[c_hash][row['hash']] = initLogger('openbmp.parsed.router.' + row['hash'],
                                                                          os.path.join(filepath, 'routers.txt'))

                if row['action'] == "init":
                    ROUTER_LOGGERS[c_hash][row['hash']].info(
                            "%-27s Action: %-9s IP: %-16s Name: %s\n"
                            "    Description: %s [%s]",
                            row['timestamp'], row['action'], row['ip_address'], row['name'],
                            row['description'], row['init_data'])

                elif row['action'] == "term":
                    ROUTER_LOGGERS[c_hash][row['hash']].info(
                            "%-27s Action: %-9s IP: %-16s Name: %s\n"
                            "    Term Reason: [%d] %s [%s]",
                            row['timestamp'], row['action'], row['ip_address'], row['name'],
                            row['term_code'], row['term_reason'], row['term_data'])
                    del ROUTER_LOGGERS[c_hash][row['hash']]

                else:
                    ROUTER_LOGGERS[c_hash][row['hash']].info(
                            "%-27s Action: %-9s IP: %-16s Name: %s",
                        row['timestamp'], row['action'], row['ip_address'], row['name'])

            except NameError as e:
                print e
                print "row = [%d] (%r)" % (len(row), row)

            except:
                pass


def processPeerMsg(msg, fdir):
    """ Process Peer message

    :param msg:      Message object
    :param fdir:        Directory where to store the parsed files
    """
    obj = Peer()

    c_hash = msg.getCollector_hash_id()
    map_list = obj.getRowMap()

    # Log messages
    for row in map_list:
        if len(row):
            try:
                # Create logger
                if c_hash not in PEER_LOGGERS or row['hash'] not in PEER_LOGGERS[c_hash]:
                    filepath = os.path.join(fdir, 'COLLECTOR_' + c_hash,
                                            'ROUTER_' + resolveIp(row['hash']))

                    try:
                        os.makedirs(filepath)
                    except:
                        pass

                    if c_hash not in PEER_LOGGERS:
                        PEER_LOGGERS[c_hash] = {}

                    PEER_LOGGERS[c_hash][row['hash']] = initLogger('openbmp.parsed.peer.' + row['hash'],
                                                                       os.path.join(filepath, 'peers.txt'))

                if row['action'] == "up":
                    PEER_LOGGERS[c_hash][row['hash']].info(
                            "%-27s Action: %-9s Name: %s [%s]\n"
                            "    Remote IP: %s:%d AS: %u BGP Id: %s HD: %u RD: %s %s %s\n"
                            "          Cap: %r\n"
                            "    Local  IP: %s:%d AS: %u BGP Id: %s HD: %u\n"
                            "          Cap: %r",
                            row['timestamp'], row['action'], row['name'], row['info_data'],
                            row['remote_ip'], row['remote_port'], row['remote_asn'], row['remote_bgp_id'],
                            row['remote_holddown'], row['peer_rd'],
                            "L3VPN" if row['isL3VPN'] else "",
                            "Pre-Policy" if row['isPrePolicy'] else "Post-Policy",
                            row['recv_cap'], row['local_ip'], row['local_port'],
                            row['local_asn'], row['local_bgp_id'], row['adv_holddown'],
                            row['adv_cap'])

                elif row['action'] == "down":
                    PEER_LOGGERS[c_hash][row['hash']].info(
                            "%-27s Action: %-9s Name: %s\n"
                            "    Remote IP: %s AS: %u BGP Id: %s RD: %s %s %s\n"
                            "    Term Info: %d bgp: %d/%d %s",
                            row['timestamp'], row['action'], row['name'],
                            row['remote_ip'], row['remote_asn'], row['remote_bgp_id'], row['peer_rd'],
                            "L3VPN" if row['isL3VPN'] else "",
                            "Pre-Policy" if row['isPrePolicy'] else "Post-Policy",
                            row['bmp_reason'], row['bgp_error_code'], row['bgp_error_sub_code'], row['error_text'])

                    del PEER_LOGGERS[c_hash][row['hash']]

                    if c_hash in BMP_STAT_LOGGERS and row['hash'] in BMP_STAT_LOGGERS[c_hash]:
                        del BMP_STAT_LOGGERS[c_hash][row['hash']]

                    if c_hash in BASE_ATTR_LOGGERS and row['hash'] in BASE_ATTR_LOGGERS[c_hash]:
                        del BASE_ATTR_LOGGERS[c_hash][row['hash']]

                    if c_hash in UNICAST_PREFIX_LOGGERS and row['hash'] in UNICAST_PREFIX_LOGGERS[c_hash]:
                        del UNICAST_PREFIX_LOGGERS[c_hash][row['hash']]

                    if c_hash in LS_NODE_LOGGERS and row['hash'] in LS_NODE_LOGGERS[c_hash]:
                        del LS_NODE_LOGGERS[c_hash][row['hash']]

                    if c_hash in LS_LINK_LOGGERS and row['hash'] in LS_LINK_LOGGERS[c_hash]:
                        del LS_LINK_LOGGERS[c_hash][row['hash']]

                    if c_hash in LS_PREFIX_LOGGERS and row['hash'] in LS_PREFIX_LOGGERS[c_hash]:
                        del LS_PREFIX_LOGGERS[c_hash][row['hash']]


                else:
                    PEER_LOGGERS[c_hash][row['hash']].info(
                            "%-27s Action: %-9s Name: %s\n"
                            "    Remote IP: %s AS: %u BGP Id: %s RD: %s %s %s",
                            row['timestamp'], row['action'], row['name'],
                            row['remote_ip'], row['remote_asn'], row['remote_bgp_id'], row['peer_rd'],
                            "L3VPN" if row['isL3VPN'] else "",
                            "Pre-Policy" if row['isPrePolicy'] else "Post-Policy")

            except NameError as e:
                print e
                print "row = [%d] (%r)" % (len(row), row)


def processBmpStatMsg(c_hash, data, fdir):
    """ Process Bmp Stats message

    :param c_hash:      Collector Hash ID
    :param data:        Message data to be consumed (should not contain headers)
    :param fdir:        Directory where to store the parsed files
    """
    obj = bmp_stat()

    # Log messages
    for row in data.split('\n'):
        if (len(row)):
            try:
                obj.parse(row)

                # Create logger
                if c_hash not in BMP_STAT_LOGGERS or obj.getPeerHashId() not in BMP_STAT_LOGGERS[c_hash]:
                    filepath = os.path.join(fdir, 'COLLECTOR_' + c_hash, 'ROUTER_' + resolveIp(obj.getRouterIp()),
                                            'PEER_' + resolveIp(obj.getPeerIp()))

                    try:
                        os.makedirs(filepath)
                    except:
                        pass

                    if c_hash not in BMP_STAT_LOGGERS:
                        BMP_STAT_LOGGERS[c_hash] = {}

                    BMP_STAT_LOGGERS[c_hash][obj.getPeerHashId()] = initLogger('openbmp.parsed.bmp_stat.' + obj.getPeerHashId(),
                                                                          os.path.join(filepath, 'bmp_stats.txt'))

                BMP_STAT_LOGGERS[c_hash][obj.getPeerHashId()].info(
                                "%-27s Pre-RIB: %-10lu Post-RIB: %-10lu Rejected: %-10u Update Dups: %-10u Withdraw Dups: %-10u\n"
                                "    Invalid Cluster List: %-10u Invalid As Path: %-10u Invalid Originator Id: %-10u Invalid AS Confed: %-10u",
                                obj.getTimestamp(),
                                obj.getPrePolicyPrefixes(), obj.getPostPolicyPrefixes(), obj.getRejectedPrefixes(), obj.getKnownDupPrefixes(),
                                obj.getKnownDupWithdrawPrefixes(), obj.getInvalidClusterListPrefixes(),
                                obj.getInvalidAsPathPrefixes(), obj.getInvalidOriginatorIdPrefixes(),
                                obj.getInvalidAsConffedPrefixes())

            except NameError as e:
                print e
                print "row = [%d] (%r)" % (len(row), row)


def processBaseAttributeMsg(c_hash, data, fdir):
    """ Process Base attribute message

    :param c_hash:      Collector Hash ID
    :param data:        Message data to be consumed (should not contain headers)
    :param fdir:        Directory where to store the parsed files
    """
    obj = base_attribute()

    # Log messages
    for row in data.split('\n'):
        if (len(row)):
            try:
                obj.parse(row)

                # Create logger
                if c_hash not in BASE_ATTR_LOGGERS or obj.getPeerHashId() not in BASE_ATTR_LOGGERS[c_hash]:
                    filepath = os.path.join(fdir, 'COLLECTOR_' + c_hash, 'ROUTER_' + resolveIp(obj.getRouterIp()),
                                            'PEER_' + resolveIp(obj.getPeerIp()))

                    try:
                        os.makedirs(filepath)
                    except:
                        pass

                    if c_hash not in BASE_ATTR_LOGGERS:
                        BASE_ATTR_LOGGERS[c_hash] = {}

                    BASE_ATTR_LOGGERS[c_hash][obj.getPeerHashId()] = initLogger('openbmp.parsed.base_attribute.' + obj.getPeerHashId(),
                                                                                os.path.join(filepath, 'base_attributes.txt'))

                BASE_ATTR_LOGGERS[c_hash][obj.getPeerHashId()].info(
                                "%-27s Origin AS: %-10u AS Count: %-6d NH: %-16s LP: %-3u MED: %-8u Origin: %s\n"
                                "    Aggregator: %-18s %s ClusterList: %-24s Originator Id: %s\n"
                                "    Path: %s %s %s",
                                obj.getTimestamp(), obj.getOriginAs(), obj.getAsPathCount(), obj.getNexthop(),
                                obj.getLocalPref(), obj.getMed(), obj.getOrigin(), obj.getAggregator(),
                                "[ Atomic ]" if obj.isAttomicAggregate() else "",
                                obj.getClusterList(), obj.getOriginatorId(), obj.getAsPath(),
                                obj.getCommunityList() + '\n' if len(obj.getClusterList()) > 0 else "",
                                obj.getExtCommunityList() + '\n' if len(obj.getExtCommunityList()) > 0 else "")

            except NameError as e:
                print "---------"
                print e
                print data
                print "row = [%d] (%r)" % (len(row), row)


def processUnicastPrefixMsg(c_hash, data, fdir):
    """ Process Unicast prefix message

    :param c_hash:      Collector Hash ID
    :param data:        Message data to be consumed (should not contain headers)
    :param fdir:        Directory where to store the parsed files
    """
    obj = unicast_prefix()

    # Log messages
    for row in data.split('\n'):
        if (len(row)):
            try:
                obj.parse(row)

                # Create logger
                if c_hash not in UNICAST_PREFIX_LOGGERS or obj.getPeerHashId() not in UNICAST_PREFIX_LOGGERS[c_hash]:
                    filepath = os.path.join(fdir, 'COLLECTOR_' + c_hash, 'ROUTER_' + resolveIp(obj.getRouterIp()),
                                            'PEER_' + resolveIp(obj.getPeerIp()))

                    try:
                        os.makedirs(filepath)
                    except:
                        pass

                    if c_hash not in UNICAST_PREFIX_LOGGERS:
                        UNICAST_PREFIX_LOGGERS[c_hash] = {}

                    UNICAST_PREFIX_LOGGERS[c_hash][obj.getPeerHashId()] = initLogger('openbmp.parsed.unicast_prefix.' + obj.getPeerHashId(),
                                                                                os.path.join(filepath, 'unicast_prefixes.txt'))

                printAggLine = True if len(obj.getClusterList()) or len(obj.getAggregator()) or len(obj.getOriginatorId()) else False

                UNICAST_PREFIX_LOGGERS[c_hash][obj.getPeerHashId()].info(
                                "%-27s Prefix: %-40s Origin AS: %-10u\n"
                                "           AS Count: %-6d NH: %-16s LP: %-3u MED: %-8u Origin: %s %s"
                                "               Path: %s %s %s",
                                obj.getTimestamp(), obj.getPrefix(),
                                obj.getOriginAs(), obj.getAsPathCount(), obj.getNexthop(),
                                obj.getLocalPref(), obj.getMed(), obj.getOrigin(),
                                "         Aggregator: %s %s ClusterList: %s Originator Id: %s\n" % (
                                                obj.getAggregator(),
                                                "[ Atomic ]" if obj.isAtomicAggregate()
                                                        else "",
                                                obj.getClusterList(), obj.getOriginatorId()),
                                obj.getAsPath(),
                                "\n        Communities: " + obj.getCommunityList() + '\n' if len(obj.getCommunityList()) > 0 else "",
                                "\n    Ext Communities: " + obj.getExtCommunityList() + '\n' if len(obj.getExtCommunityList()) > 0 else "")

            except NameError as e:

                print "++++++"
                print e
                print data
                print "row = [%d] (%r)" % (len(row), row)


def processLsNodeMsg(c_hash, data, fdir):
    """ Process LS Node message

    :param c_hash:      Collector Hash ID
    :param data:        Message data to be consumed (should not contain headers)
    :param fdir:        Directory where to store the parsed files
    """
    obj = ls_node()

    # Log messages
    for row in data.split('\n'):
        if (len(row)):
            try:
                obj.parse(row)

                # Create logger
                if c_hash not in LS_NODE_LOGGERS or obj.getPeerHashId() not in LS_NODE_LOGGERS[c_hash]:
                    filepath = os.path.join(fdir, 'COLLECTOR_' + c_hash, 'ROUTER_' + resolveIp(obj.getRouterIp()),
                                            'PEER_' + resolveIp(obj.getPeerIp()))

                    try:
                        os.makedirs(filepath)
                    except:
                        pass

                    if c_hash not in LS_NODE_LOGGERS:
                        LS_NODE_LOGGERS[c_hash] = {}

                    LS_NODE_LOGGERS[c_hash][obj.getPeerHashId()] = initLogger('openbmp.parsed.ls_node.' + obj.getPeerHashId(),
                                                                          os.path.join(filepath, 'ls_nodes.txt'))

                LS_NODE_LOGGERS[c_hash][obj.getPeerHashId()].info(
                                "%-27s RID: %-46s Hash Id: %-32s RoutingID: 0x%s Name: %s\n"
                                "    Proto: %-10s LsId: 0x%s MT Id: %-7u Area: %s Flags: %s\n"
                                "    AS Path: %s LP: %u MED: %u NH: %s",
                                obj.getTimestamp(), obj.getIgpRouterId() + '/' + obj.getRouterId(),
                                obj.getHashId(), obj.getRoutingId(), obj.getName(), obj.getProtocol(), obj.getLsId(),
                                obj.getMtId(), obj.getOspfAreaId() if len(obj.getOspfAreaId()) else obj.getIsisAreaId(),
                                obj.getFlags(), obj.getAsPath(), obj.getLocalPref(), obj.getMed(), obj.getNexthop())

            except NameError as e:
                print "----"
                print e
                print data
                print "ls_node: row = [%d] (%r)" % (len(row), row)


def processLsLinkMsg(c_hash, data, fdir):
    """ Process LS Link message

    :param c_hash:      Collector Hash ID
    :param data:        Message data to be consumed (should not contain headers)
    :param fdir:        Directory where to store the parsed files
    """
    obj = ls_link()

    # Log messages
    for row in data.split('\n'):
        if (len(row)):
            try:
                obj.parse(row)

                # Create logger
                if c_hash not in LS_LINK_LOGGERS or obj.getPeerHashId() not in LS_LINK_LOGGERS[c_hash]:
                    filepath = os.path.join(fdir, 'COLLECTOR_' + c_hash, 'ROUTER_' + resolveIp(obj.getRouterIp()),
                                            'PEER_' + resolveIp(obj.getPeerIp()))

                    try:
                        os.makedirs(filepath)
                    except:
                        pass

                    if c_hash not in LS_LINK_LOGGERS:
                        LS_LINK_LOGGERS[c_hash] = {}

                    LS_LINK_LOGGERS[c_hash][obj.getPeerHashId()] = initLogger('openbmp.parsed.ls_link.' + obj.getPeerHashId(),
                                                                          os.path.join(filepath, 'ls_links.txt'))

                LS_LINK_LOGGERS[c_hash][obj.getPeerHashId()].info(
                                "%-27s RID: %-46s Hash Id: %-32s RoutingID: 0x%s \n"
                                "    Proto: %-10s LsId: 0x%s MT Id: %s Area: %s Adm Grp: %u SRLG: %s Name: %s\n"
                                "    Metric: %u Link Id (local/remote): %u/%u Interface IP (local/remote): %s/%s"
                                "    Node Hash Id (local/remote): %s/%s\n"
                                "    AS Path: %s LP: %u MED: %u NH: %s %s",
                                obj.getTimestamp(), obj.getIgpRouterId() + '/' + obj.getRouterId(),
                                obj.getHashId(), obj.getRoutingId(), obj.getProtocol(), obj.getLsId(),
                                obj.getMtId(), obj.getOspfAreaId() if len(obj.getOspfAreaId()) else obj.getIsisAreaId(),
                                obj.getAdminGroup(), obj.getSrlg(), obj.getLinkName(),
                                obj.getIgpMetric(),obj.getLocalLinkId(), obj.getRemoteLinkId(), obj.getInterfaceIp(),
                                obj.getNeighborIp(),obj.getLocalNodeHashId(), obj.getRemoteNodeHashId(),
                                obj.getAsPath(), obj.getLocalPref(), obj.getMed(),obj.getNexthop(),
                                "\n    MPLS Proto: %s Protection: %s TE Metric: %u Max Link BW: %f Max Resv BW: %f Unresv BW: %s" %
                                    (obj.getMplsProtoMask(), obj.getLinkProtection(), obj.getTeDefaultMetric(),
                                     obj.getMaxLinkBw(), obj.getMaxResvBw(), obj.getUnreservedBw()) if obj.getTeDefaultMetric() else "")

            except NameError as e:
                print "----"
                print e
                print data
                print "ls_link: row = [%d] (%r)" % (len(row), row)


def processLsPrefixMsg(c_hash, data, fdir):
    """ Process LS Prefix message

    :param c_hash:      Collector Hash ID
    :param data:        Message data to be consumed (should not contain headers)
    :param fdir:        Directory where to store the parsed files
    """
    obj = ls_prefix()

    # Log messages
    for row in data.split('\n'):
        if (len(row)):
            try:
                obj.parse(row)

                # Create logger
                if c_hash not in LS_PREFIX_LOGGERS or obj.getPeerHashId() not in LS_PREFIX_LOGGERS[c_hash]:
                    filepath = os.path.join(fdir, 'COLLECTOR_' + c_hash, 'ROUTER_' + resolveIp(obj.getRouterIp()),
                                            'PEER_' + resolveIp(obj.getPeerIp()))

                    try:
                        os.makedirs(filepath)
                    except:
                        pass

                    if c_hash not in LS_PREFIX_LOGGERS:
                        LS_PREFIX_LOGGERS[c_hash] = {}

                    LS_PREFIX_LOGGERS[c_hash][obj.getPeerHashId()] = initLogger('openbmp.parsed.ls_prefix.' + obj.getPeerHashId(),
                                                                          os.path.join(filepath, 'ls_prefixes.txt'))

                LS_PREFIX_LOGGERS[c_hash][obj.getPeerHashId()].info(
                                "%-27s RID: %-46s Prefix: %-32s Hash Id: %-32s RoutingID: 0x%s \n"
                                "    Proto: %-10s LsId: 0x%s MT Id: %s Area: %s %s %s\n"
                                "    Metric: %u Flags: %s RTag: %u Ext RTag: 0x%s"
                                "    Local Node Hash Id: %s AS Path: %s LP: %u MED: %u NH: %s",
                                obj.getTimestamp(), obj.getIgpRouterId() + '/' + obj.getRouterId(),
                                obj.getPrefix(), obj.getHashId(), obj.getRoutingId(), obj.getProtocol(), obj.getLsId(),
                                obj.getMtId(), obj.getOspfAreaId() if len(obj.getOspfAreaId()) else obj.getIsisAreaId(),
                                " Route Type: %s" % obj.getOspfRouteType() if len(obj.getOspfRouteType()) else "",
                                " Fwd Addr: %s" % obj.getOspfFwdAddress() if len(obj.getOspfFwdAddress()) else "",
                                obj.getIgpMetric(), obj.getIgpFlags(), obj.getRouteTag(), obj.getExtRouteTag(),
                                obj.getLocalNodeHashId(), obj.getAsPath(), obj.getLocalPref(), obj.getMed(), obj.getNexthop())

            except NameError as e:
                print "----"
                print e
                print data
                print "ls_prefix: row = [%d] (%r)" % (len(row), row)


def processBmpRawMsg(c_hash, r_hash, r_ip, data, fdir):
    """ Process BMP RAW message

    :param c_hash:      Collector Hash ID
    :param r_hash:      Router Hash ID
    :param r_ip:        Router IP address
    :param data:        Message data to be consumed (should not contain headers)
    :param fdir:        Directory where to store the parsed files
    """
    # Create logger
    if c_hash not in BMP_RAW_LOGGERS or r_hash not in BMP_RAW_LOGGERS[c_hash]:
        filepath = os.path.join(fdir, 'COLLECTOR_' + c_hash, 'ROUTER_' + resolveIp(r_ip))

        try:
            os.makedirs(filepath)
        except:
            pass

        if c_hash not in BMP_RAW_LOGGERS:
            BMP_RAW_LOGGERS[c_hash] = {}

        BMP_RAW_LOGGERS[c_hash][r_hash] = init_raw_logger("openbmp.bmp_raw." + r_hash,
                                                          os.path.join(filepath, "bmp_feed.raw"))

    if BMP_RAW_LOGGERS[c_hash][r_hash]:
        BMP_RAW_LOGGERS[c_hash][r_hash].info(data)


def processMessage(msg, fdir):
    """ Process the message

    :param msg:     Message consumed
    :param fdir:         Directory where to store the parsed files

    :return:
    """
    try:
        m = Message(msg.value)  # Gets body of kafka message.

        if msg.topic == 'openbmp.parsed.collector':
            processCollectorMsg(m, fdir)

        elif msg.topic == 'openbmp.parsed.router':
            processRouterMsg(m, fdir)

        elif msg.topic == 'openbmp.parsed.peer':
            processPeerMsg(m, fdir)

        elif msg.topic == 'openbmp.parsed.bmp_stat':
            processBmpStatMsg(m, fdir)

        elif msg.topic == 'openbmp.parsed.base_attribute':
            processBaseAttributeMsg(m, fdir)

        elif msg.topic == 'openbmp.parsed.unicast_prefix':
            processUnicastPrefixMsg(m, fdir)

        elif msg.topic == 'openbmp.parsed.ls_node':
            processLsNodeMsg(m, fdir)

        elif msg.topic == 'openbmp.parsed.ls_link':
            processLsLinkMsg(m, fdir)

        elif msg.topic == 'openbmp.parsed.ls_prefix':
            processLsPrefixMsg(m, fdir)

        elif msg.topic == 'openbmp.bmp_raw':
            processBmpRawMsg(m, fdir)

    except:
        print "ERROR processing message: "
        traceback.print_exc()
        print msg


def init_raw_logger(name, filename):
    """ Initialize a new logger instance

    :param name:        name of logger
    :param filename:    Filename for the logger

    :return: logger instance or None if config is missing
    """
    logger = None

    if CONFIG['logging']['bytime']['when']:
        logger = logging.getLogger(name)

        for h in logger.handlers:
            logger.removeHandler(h)

        logger.setLevel(logging.INFO)

        handler = RawTimedRotatingFileHandler(
            filename=filename,
            when=CONFIG['logging']['bytime']['when'],
            interval=CONFIG['logging']['bytime']['interval'],
            backupCount=CONFIG['logging']['bytime']['backupCount'],
            delay=True)

        # Set the handler so we can remove it
        logger._handler = handler

        logger.addHandler(handler)

    return logger


def initLogger(name, filename):
    """ Initialize a new logger instance

    :param name:        name of logger
    :param filename:    Filename for the logger

    :return: logger instance
    """
    logger = logging.getLogger(name)

    for h in logger.handlers:
        logger.removeHandler(h)

    logger.setLevel(logging.INFO)

    if CONFIG['logging']['rotate'] == "bysize":
        handler = logging.handlers.RotatingFileHandler(
                            filename=filename,
                            maxBytes=CONFIG['logging']['bysize']['maxBytes'],
                            backupCount=CONFIG['logging']['bysize']['backupCount'],
                            delay=True)

    elif CONFIG['logging']['rotate'] == "bytime":
        handler = logging.handlers.TimedRotatingFileHandler(
                            filename=filename,
                            when=CONFIG['logging']['bytime']['when'],
                            interval=CONFIG['logging']['bytime']['interval'],
                            backupCount=CONFIG['logging']['bytime']['backupCount'],
                            delay=True)

    formatter = logging.Formatter('%(message)s')
    handler.setFormatter(formatter)

    # Set the handler so we can remove it
    logger._handler = handler

    logger.addHandler(handler)

    return logger


def resolveIp(addr):
    """ Resolves an IP address to FQDN.

    :param addr:        IPv4/v6 address to resovle
    :return: FQDN or IP address if FQDN unknown/not found
    """
    try:
        return socket.gethostbyaddr(addr)[0]
    except:
        return addr


def usage(prog):
    """ Usage - Prints the usage for this program.

        :param prog:  Program name
    """
    print ""
    print "Usage: %s [OPTIONS]" % prog
    print ""

    print "OPTIONS:"
    print "  -h, --help".ljust(30) + "Print this help menu"
    print "  -c, --config".ljust(30) + "Config filename (default is %s/etc/openbmp-file-consumer.yml)" % sys.prefix
    print ""


def load_config(cfg_filename):
    """ Load and validate the configuration from YAML

        Some defaults are applied if any settings are missing.
    """
    global CONFIG

    try:
        CONFIG = yaml.load(file(cfg_filename, 'r'))

        # Validate the config and set defaults if undefined
        if 'kafka' in CONFIG:
            if 'servers' not in CONFIG['kafka']:
                CONFIG['kafka']['servers'] = ['localhost:9092']
            if 'client_id' not in CONFIG['kafka']:
                CONFIG['kafka']['client_id'] = 'openbmp-file-consumer'
            if 'group_id' not in CONFIG['kafka']:
                CONFIG['kafka']['group_id'] = 'openbmp-file-consumer'
            if 'offset_reset_largest' not in CONFIG['kafka']:
                CONFIG['kafka']['offset_reset_largest'] = True

        else:
            CONFIG['kafka'] = {}
            CONFIG['kafka']['servers'] = ['localhost:9092']
            CONFIG['kafka']['client_id'] = 'openbmp-file-consumer'
            CONFIG['kafka']['group_id'] = 'openbmp-file-consumer'
            CONFIG['kafka']['offset_reset_largest'] = True

        if 'topic' in CONFIG:
            if 'bmp_raw' not in CONFIG['topic']:
                CONFIG['topic']['bmp_raw'] = True
            if 'link_state' not in CONFIG['topic']:
                CONFIG['topic']['link_state'] = True
            if 'base_attribute' not in CONFIG['topic']:
                CONFIG['topic']['base_attribute'] = True
            if 'unicast_prefix' not in CONFIG['topic']:
                CONFIG['topic']['unicast_prefix'] = True

        else:
            CONFIG['topic'] = {}
            CONFIG['topic']['bmp_raw'] = True
            CONFIG['topic']['link_state'] = True
            CONFIG['topic']['base_attribute'] = True
            CONFIG['topic']['unicast_prefix'] = True

        if 'collector' in CONFIG:
            if 'heartbeat' in CONFIG['collector']:
                if 'interval' not in CONFIG['collector']['heartbeat']:
                    CONFIG['collector']['heartbeat']['interval'] = 14430
            else:
                CONFIG['collector'] = {'heartbeat': {'interval': 14430}}
        else:
            CONFIG['collector'] = {'heartbeat': {'interval': 14430}}

        if 'logging' in CONFIG:
            if 'base_path' not in CONFIG['logging']:
                CONFIG['logging']['base_path'] = '/tmp/openbmp-log'

            if not os.path.exists(CONFIG['logging']['base_path']):
                print "Base path '%s' does not exist, attempting to create it" % CONFIG['logging']['base_path']
                os.mkdir(CONFIG['logging']['base_path'], 0755)

            if 'rotate' not in CONFIG['logging']:
                CONFIG['logging']['rotate'] = 'bysize'

            # Make sure the rotate config is present
            if CONFIG['logging']['rotate'] not in CONFIG['logging']:
               CONFIG['logging'] = {
                        'base_path': '/var/openbmp-log',
                        'bysize': {'maxBytes': 100000000, 'backupCount': 20},
                        'rotate': 'bysize'
                    }

            # based on the rotate method, make sure required vars are defined
            if 'bysize' in CONFIG['logging']['rotate']:
                if 'maxBytes' not in CONFIG['logging']['bysize']:
                    CONFIG['logging']['bysize']['maxBytes'] = 100000000
                if 'backupCount' not in CONFIG['logging']['bysize']:
                    CONFIG['logging']['bysize']['backupCount'] = 20

            elif 'bytime' in CONFIG['logging']['rotate']:
                if 'when' not in CONFIG['logging']['bytime']:
                    CONFIG['logging']['bytime']['when'] = 'h'
                if 'interval' not in CONFIG['logging']['bytime']:
                    CONFIG['logging']['bytime']['interval'] = 48
                if 'backupCount' not in CONFIG['logging']['bysize']:
                    CONFIG['logging']['bytime']['backupCount'] = 20

        else:
            CONFIG['logging'] = {
                        'base_path': '/tmp/openbmp-log',
                        'bysize': {'maxBytes': 100000000, 'backupCount': 20},
                        'rotate': 'bysize'
                    }

    except (IOError, yaml.YAMLError), e:
        print ("Failed to load mapping config file %s : %r" % (cfg_filename, e))
        if hasattr(e, 'problem_mark'):
            mark = e.problem_mark
            print ("error on line: %s, column: %s" % (mark.line+1, mark.column+1))

        sys.exit(2)


def parseCmdArgs(argv):
    """ Parse commandline arguments and load the configuration file

        Usage is printed and program is terminated if there is an error.

        :param argv:   ARGV as provided by sys.argv.  Arg 0 is the program name
    """
    cfg_filename = "%s/etc/openbmp-file-consumer.yml" % sys.prefix

    try:
        (opts, args) = getopt.getopt(argv[1:], "hc:",
                                       ["help", "config="])

        for o, a in opts:
            if o in ("-h", "--help"):
                usage(argv[0])
                sys.exit(0)

            elif o in ("-c", "--config"):
                cfg_filename = a

            else:
                usage(argv[0])
                sys.exit(1)

        load_config(cfg_filename)

    except getopt.GetoptError as err:
        print str(err)  # will print something like "option -a not recognized"
        usage(argv[0])
        sys.exit(2)


def main():
    """ Main entry point for shell script """
    parseCmdArgs(sys.argv)

    # Enable to topics/feeds
    topics = [ 'openbmp.parsed.collector', 'openbmp.parsed.router',
               'openbmp.parsed.peer', 'openbmp.parsed.bmp_stat']

    if CONFIG['topic']['base_attribute']:
        topics.append('openbmp.parsed.base_attribute')

    if CONFIG['topic']['unicast_prefix']:
        topics.append('openbmp.parsed.unicast_prefix')

    if CONFIG['topic']['link_state']:
        topics.append('openbmp.parsed.ls_node')
        topics.append('openbmp.parsed.ls_link')
        topics.append('openbmp.parsed.ls_prefix')

    if CONFIG['topic']['bmp_raw']:
        topics.append('openbmp.bmp_raw')

    try:
        # connect and bind to topics
        print "Connecting to %r ... takes a minute to load offsets and topics, please wait" % CONFIG['kafka']['servers']
        consumer = kafka.KafkaConsumer(
                            *topics,
                            bootstrap_servers=CONFIG['kafka']['servers'],
                            client_id=CONFIG['kafka']['client_id'],
                            group_id=CONFIG['kafka']['group_id'],
                            enable_auto_commit=True,
                            auto_commit_interval_ms=1000,
                            auto_offset_reset="largest" if CONFIG['kafka']['offset_reset_largest'] else "smallest")

        print "Connected, now consuming"
        print "Logs are stored under '%s'" % CONFIG['logging']['base_path']

        while True:
            for m in consumer:
                processMessage(m, CONFIG['logging']['base_path'])

            # Check collector status
            cur_ts = time.time()

            for c in COLLECTOR_ADMIN_ID:
                (aid, ts) = COLLECTOR_ADMIN_ID[c]

                if (cur_ts - ts) >= CONFIG['collector']['heartbeat']['interval']:
                    COLLECTOR_LOGGERS[c].error("%10s | %27s | %32s | " % ("dead",
                                               datetime.datetime.fromtimestamp(cur_ts).strftime("%Y-%m-%d %H:%M:%S.%f"),
                                               aid))

                    try:
                        del COLLECTOR_LOGGERS[c]
                        del COLLECTOR_ADMIN_ID[c]
                        del ROUTER_LOGGERS[c]
                        del PEER_LOGGERS[c]

                        if c in BMP_STAT_LOGGERS:
                            del BMP_STAT_LOGGERS[c]

                        if c in UNICAST_PREFIX_LOGGERS:
                            del UNICAST_PREFIX_LOGGERS[c]

                        if c in BASE_ATTR_LOGGERS:
                            del BASE_ATTR_LOGGERS[c]

                        if c in BMP_RAW_LOGGERS:
                            for r in BMP_RAW_LOGGERS[c]:
                                for f in BMP_RAW_LOGGERS[c][r]:
                                    f.close()
                    except:
                        pass

                    break

    except kafka.common.KafkaUnavailableError as err:
        print "Kafka Error: %s" % str(err)

    except KeyboardInterrupt:
        print "User stop requested"


if __name__ == '__main__':
    main()
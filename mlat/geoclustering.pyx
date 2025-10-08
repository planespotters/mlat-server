#!python
#cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False

# -*- mode: python; indent-tabs-mode: nil -*-

# Part of mlat-server: a Mode S multilateration server
# Copyright (C) 2025  Thomas Noack <thomas.noack@planespotters.net>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
Geographic clustering of receivers based on proximity.

Automatically groups receivers that are close together to optimize
clock synchronization operations.
"""

import time
import logging
from mlat import geodesy

glogger = logging.getLogger("geoclustering")


cdef class ReceiverCluster:
    """A geographic cluster of receivers that are close together"""

    cdef public int cluster_id
    cdef public double center_lat
    cdef public double center_lon
    cdef public object center_ecef
    cdef public set receivers
    cdef public double last_updated

    def __init__(self, int cluster_id, double center_lat, double center_lon):
        self.cluster_id = cluster_id
        self.center_lat = center_lat
        self.center_lon = center_lon
        self.receivers = set()
        self.last_updated = time.time()
        self._update_center_ecef()

    cdef _update_center_ecef(self):
        """Update ECEF position of cluster center"""
        self.center_ecef = geodesy.llh2ecef([self.center_lat, self.center_lon, 0])

    def add_receiver(self, receiver):
        """Add a receiver to this cluster"""
        self.receivers.add(receiver)
        if not hasattr(receiver, 'clusters'):
            receiver.clusters = set()
        receiver.clusters.add(self)

    def remove_receiver(self, receiver):
        """Remove a receiver from this cluster"""
        self.receivers.discard(receiver)
        if hasattr(receiver, 'clusters'):
            receiver.clusters.discard(self)

    def recompute_center(self):
        """Recompute cluster center as centroid of all receivers"""
        if not self.receivers:
            return

        cdef double lat_sum = 0.0
        cdef double lon_sum = 0.0
        cdef int count = 0

        for receiver in self.receivers:
            lat_sum += receiver.position_llh[0]
            lon_sum += receiver.position_llh[1]
            count += 1

        if count > 0:
            self.center_lat = lat_sum / count
            self.center_lon = lon_sum / count
            self._update_center_ecef()
            self.last_updated = time.time()

    def __repr__(self):
        return f'ReceiverCluster(id={self.cluster_id}, size={len(self.receivers)}, center=({self.center_lat:.2f},{self.center_lon:.2f}))'


cdef class ReceiverClusterManager:
    """Manages dynamic clustering of receivers based on geographic proximity"""

    cdef public list clusters
    cdef public dict receiver_to_clusters
    cdef public int next_cluster_id
    cdef public double cluster_threshold
    cdef public double cluster_buffer

    def __init__(self, double cluster_threshold=500e3):
        self.clusters = []
        self.receiver_to_clusters = {}
        self.next_cluster_id = 0
        self.cluster_threshold = cluster_threshold
        self.cluster_buffer = 100e3  # 100km buffer for cluster edge receivers

    def add_receiver(self, receiver):
        """Add receiver to appropriate cluster(s) or create new cluster

        Returns list of clusters the receiver was assigned to.
        """
        cdef list assigned_clusters = []
        cdef object receiver_pos = receiver.position
        cdef double distance

        # Find all clusters within threshold
        for cluster in self.clusters:
            distance = geodesy.ecef_distance(receiver_pos, cluster.center_ecef)
            if distance < self.cluster_threshold:
                cluster.add_receiver(receiver)
                assigned_clusters.append(cluster)

        # If not assigned to any cluster, create a new one
        if not assigned_clusters:
            new_cluster = ReceiverCluster(
                self.next_cluster_id,
                receiver.position_llh[0],
                receiver.position_llh[1]
            )
            self.next_cluster_id += 1
            new_cluster.add_receiver(receiver)
            self.clusters.append(new_cluster)
            assigned_clusters.append(new_cluster)

            glogger.info(f"Created new cluster {new_cluster.cluster_id} for {receiver.user} at ({new_cluster.center_lat:.2f}, {new_cluster.center_lon:.2f})")

        self.receiver_to_clusters[receiver] = set(assigned_clusters)
        return assigned_clusters

    def remove_receiver(self, receiver):
        """Remove receiver from all its clusters"""
        cdef set clusters = self.receiver_to_clusters.pop(receiver, set())

        for cluster in clusters:
            cluster.remove_receiver(receiver)

        # Remove empty clusters
        cdef list empty_clusters = [c for c in self.clusters if not c.receivers]
        for cluster in empty_clusters:
            glogger.info(f"Removing empty cluster {cluster.cluster_id}")
            self.clusters.remove(cluster)

    def get_nearby_receivers(self, position_ecef, double threshold=0):
        """Get all receivers in clusters near given position

        Args:
            position_ecef: ECEF position to search near
            threshold: Distance threshold in meters (default: use cluster_threshold + buffer)

        Returns:
            Set of receivers within threshold
        """
        if threshold == 0:
            threshold = self.cluster_threshold + self.cluster_buffer

        cdef set nearby_receivers = set()
        cdef double distance

        for cluster in self.clusters:
            distance = geodesy.ecef_distance(position_ecef, cluster.center_ecef)
            if distance < threshold:
                nearby_receivers.update(cluster.receivers)

        return nearby_receivers

    def get_clusters_near_position(self, position_ecef, double threshold=0):
        """Get all clusters near given position

        Args:
            position_ecef: ECEF position to search near
            threshold: Distance threshold in meters (default: use cluster_threshold + buffer)

        Returns:
            List of clusters within threshold
        """
        if threshold == 0:
            threshold = self.cluster_threshold + self.cluster_buffer

        cdef list nearby_clusters = []
        cdef double distance

        for cluster in self.clusters:
            distance = geodesy.ecef_distance(position_ecef, cluster.center_ecef)
            if distance < threshold:
                nearby_clusters.append(cluster)

        return nearby_clusters

    def recompute_all_clusters(self):
        """Recompute centers of all clusters - call periodically"""
        for cluster in self.clusters:
            cluster.recompute_center()

    def get_statistics(self):
        """Get clustering statistics for monitoring

        Returns:
            dict with cluster statistics
        """
        if not self.clusters:
            return {
                'num_clusters': 0,
                'avg_cluster_size': 0,
                'min_cluster_size': 0,
                'max_cluster_size': 0,
                'total_receivers': 0
            }

        cdef list cluster_sizes = [len(c.receivers) for c in self.clusters]
        cdef int total_receivers = sum(cluster_sizes)

        return {
            'num_clusters': len(self.clusters),
            'avg_cluster_size': total_receivers / len(self.clusters) if self.clusters else 0,
            'min_cluster_size': min(cluster_sizes) if cluster_sizes else 0,
            'max_cluster_size': max(cluster_sizes) if cluster_sizes else 0,
            'total_receivers': total_receivers
        }

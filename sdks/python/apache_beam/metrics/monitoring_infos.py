#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# cython: language_level=3
# cython: profile=True

from __future__ import absolute_import
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.metrics.cells import DistributionData
from apache_beam.metrics.cells import DistributionResult
from google.protobuf import timestamp_pb2

from apache_beam.metrics.execution import MetricKey
import logging
import time


# TODO relcoate sshared helper for to_timestamp_pb2
# timestamp_secs is a float
def to_timestamp_proto(timestamp_secs):
  seconds = int(timestamp_secs)
  nanos = int((timestamp_secs - seconds) * 10**9)
  return timestamp_pb2.Timestamp(seconds=seconds, nanos=nanos)

def to_timestamp_secs(timestamp_proto):
  return timestamp_proto.seconds + timestamp_proto.nanos * 10**-9

def extract_value(monitoring_info):
  if is_counter(monitoring_info):
    return monitoring_info.metric.counter_data.int64_value

def extract_timestamp(monitoring_info):
  return monitoring_info.timestamp

def extract_distribution(monitoring_info):
  # TODO handle double_distribution_data
  if is_distribution(monitoring_info):
    return monitoring_info.metric.distribution_data.int_distribution_data

# TODO determine what the value is and store it as an int, double, etc.
def ptransform_int64_metric(urn, value, ptransform='', tag=''):
  labels = {}
  if tag:
    labels['TAG'] = tag # TODO do we want TAG? put it in the base 
  metric = beam_fn_api_pb2.Metric(
      counter_data=beam_fn_api_pb2.CounterData(
          int64_value=value
      )
  )
  return create_monitoring_info(
      metric, urn, 'beam:metrics:SumInt64', ptransform, labels)

def ptransform_int64_distribution(urn, dist_data, ptransform='', tag=''):
  labels = {}
  if tag:
    labels['TAG'] = tag # TODO do we want TAG? put it in the base 
  metric = beam_fn_api_pb2.Metric(distribution_data=dist_data)
  return create_monitoring_info(
      metric, urn, 'beam:metrics:DistributionInt64', ptransform, labels)

def ptransform_int64_gauge(urn, gauge_counter_data, ptransform='', tag=''):
  labels = {}
  if tag:
    labels['TAG'] = tag # TODO do we want TAG? put it in the base 
  metric = beam_fn_api_pb2.Metric(counter_data=gauge_counter_data)
  return create_monitoring_info(
      metric, urn, 'beam:metrics:LatestInt64', ptransform, labels)

def create_monitoring_info(metric, urn, type_urn, ptransform, labels=dict()):
  if not ptransform:
    ptransform = 'PLACEHOLDER'
  labels['PTRANSFORM'] = ptransform
  return beam_fn_api_pb2.MonitoringInfo(
      urn=urn,
      type=type_urn,
      labels=labels,
      metric=metric,
      timestamp=to_timestamp_proto(time.time())
  )

def user_metric_urn(namespace, name):
  return 'beam:metric:user:%s:%s' % (namespace, name)

def to_metric_key(monitoring_info):
  # Right now this assumes that all metrics have a PTRANSFORM
  ptransform_id = monitoring_info.labels['PTRANSFORM']  
  # TODO do we need to parse out the user name, namespace, etc.
  return MetricKey(ptransform_id, monitoring_info.urn)

def is_counter(monitoring_info):
  return monitoring_info.type in ['beam:metrics:SumInt64']

def is_distribution(monitoring_info):
  return monitoring_info.type in ['beam:metrics:DistributionInt64']

def is_gauge(monitoring_info):
  return monitoring_info.type in ['beam:metrics:LatestInt64']

def to_metric_result(monitoring_info):
  # Returns a metric result (AKA the legacy format).
  # from the MonitoringInfo
  if is_counter(monitoring_info):
    return beam_fn_api_pb2.User.CounterData(
        value=extract_value(monitoring_info))
  if is_distribution(monitoring_info):
    distribution_data = extract_distribution(monitoring_info)
    return metrics.cells.DistributionResult(
        DistributionData(distribution_data.sum, distribution_data.count,
                         distribution_data.min, distribution_data.max))
  if is_gauge(monitoring_info):
    timestamp_secs = to_timestamp_secs(extract_timestamp(monitoring_info))
    return GaugeResult(GaugeData(
        extract_value(monitoring_info), timestamp_secs))

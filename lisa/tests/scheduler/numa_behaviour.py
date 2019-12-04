# SPDX-License-Identifier: Apache-2.0
#
# Copyright (C) 2019, Linaro and contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import os.path

import pandas as pd

from lisa.wlgen.rta import Periodic
from lisa.tests.base import ResultBundle, RTATestBundle
from lisa.utils import ArtifactPath
from lisa.trace import requires_events
from lisa.target import Target
from lisa.trace import FtraceCollector
from lisa.datautils import df_filter_task_ids

class NUMABehaviour(RTATestBundle):
    """
    Abstract class for NUMA related scheduler testing.

    This class provides :meth:`test_task_placement` to validate the basic
    behaviour of NUMA related scheduling decisions. 
    """

    @classmethod
    def _from_target(cls, target: Target, *, res_dir: ArtifactPath = None, ftrace_coll: FtraceCollector = None) -> 'NUMABehaviour':
        """
        Factory method to create a bundle using a live target

        This will execute the rt-app workload described in
        :meth:`lisa.tests.base.RTATestBundle.get_rtapp_profile`
        """
        plat_info = target.plat_info
        rtapp_profile = cls.get_rtapp_profile(plat_info)

        cls.run_rtapp(target, res_dir, rtapp_profile, ftrace_coll=ftrace_coll)

        return cls(res_dir, plat_info)

    def _get_task_cpu_df(self, task):
        """
        Get a DataFrame for task mirgrations

        Use the sched_switch trace event to find task mirgation from one CPU to another.

        :returns: A Pandas DataFrame for the task, showing the
                  new CPU that the task was migrated to
        """        
        df = self.trace.df_events('sched_switch')[['next_pid', 'next_comm', '__cpu']]
        task_id = self.trace.get_task_id(task, update=False)
        _df = df_filter_task_ids(df, [task_id], pid_col='next_pid', comm_col='next_comm')
        cpu_df = _df.drop_duplicates(subset='__cpu', keep='first', inplace=False)
                
        return cpu_df

    @requires_events('sched_switch')
    def test_task_placement(self) -> ResultBundle:
        """
        Test that task remained on the same core 

        """
        test_passed = True
        failed_tasks = []
        
        for task in self.rtapp_tasks:
            cpu_df = self._get_task_cpu_df(task)
            core_migrations = len(cpu_df.index)
            # Ideally, task with low utilization
            # should stay on the same core and
            # do not cross NUMA boundaries
            if  core_migrations > 1:
                test_passed = False
                failed_tasks.append("{} core_migrations {}".format(task, core_migrations))

        res = ResultBundle.from_bool(test_passed)
        res.add_metric("Failed tasks", failed_tasks)
        
        return res

class NUMA_SmallTaskPlacement(NUMABehaviour):
    """
    A single task with 50% utilization
    """

    task_prefix = "ntask"

    @classmethod
    def get_rtapp_profile(cls, plat_info):
        rtapp_profile = {}
        rtapp_profile[cls.task_prefix] = Periodic(
            duty_cycle_pct=50,
            duration_s=30,
            period_ms=cls.TASK_PERIOD_MS
        )

        return rtapp_profile
    
class NUMA_MultipleTasksPlacement(NUMABehaviour):
    """
    Multiple tasks with 50% utilization
    """
    task_prefix = "ntask"
    
    @classmethod
    def get_rtapp_profile(cls, plat_info):
        # Four CPU's is enough to demonstrate task migration problem
        cpu_count = min(4, plat_info["cpus-count"])        

        rtapp_profile = {}
        for cpu in range(cpu_count):            
            rtapp_profile["{}_{}".format(cls.task_prefix, cpu)] = Periodic(
                duty_cycle_pct=50,
                duration_s=30,
                period_ms=cls.TASK_PERIOD_MS
            )

        return rtapp_profile    
# vim :set tabstop=4 shiftwidth=4 textwidth=80 expandtab
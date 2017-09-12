#    Copyright 2013-2015 ARM Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


"""
Utility functions for working with Android devices through adb.

"""
# pylint: disable=E1103
import os
import time
import subprocess
import logging
import re
import threading
import tempfile
import Queue
from collections import defaultdict

from devlib.exception import TargetError, HostError, DevlibError
from devlib.utils.misc import check_output, which, memoized, ABI_MAP
from devlib.utils.misc import escape_single_quotes, escape_double_quotes
from devlib import host


logger = logging.getLogger('android')

MAX_ATTEMPTS = 5
AM_START_ERROR = re.compile(r"Error: Activity.*")

# See:
# http://developer.android.com/guide/topics/manifest/uses-sdk-element.html#ApiLevels
ANDROID_VERSION_MAP = {
    23: 'MARSHMALLOW',
    22: 'LOLLYPOP_MR1',
    21: 'LOLLYPOP',
    20: 'KITKAT_WATCH',
    19: 'KITKAT',
    18: 'JELLY_BEAN_MR2',
    17: 'JELLY_BEAN_MR1',
    16: 'JELLY_BEAN',
    15: 'ICE_CREAM_SANDWICH_MR1',
    14: 'ICE_CREAM_SANDWICH',
    13: 'HONEYCOMB_MR2',
    12: 'HONEYCOMB_MR1',
    11: 'HONEYCOMB',
    10: 'GINGERBREAD_MR1',
    9: 'GINGERBREAD',
    8: 'FROYO',
    7: 'ECLAIR_MR1',
    6: 'ECLAIR_0_1',
    5: 'ECLAIR',
    4: 'DONUT',
    3: 'CUPCAKE',
    2: 'BASE_1_1',
    1: 'BASE',
}


# Initialized in functions near the botton of the file
android_home = None
platform_tools = None
adb = None
aapt = None
fastboot = None


class AndroidProperties(object):

    def __init__(self, text):
        self._properties = {}
        self.parse(text)

    def parse(self, text):
        self._properties = dict(re.findall(r'\[(.*?)\]:\s+\[(.*?)\]', text))

    def iteritems(self):
        return self._properties.iteritems()

    def __iter__(self):
        return iter(self._properties)

    def __getattr__(self, name):
        return self._properties.get(name)

    __getitem__ = __getattr__


class AdbDevice(object):

    def __init__(self, name, status):
        self.name = name
        self.status = status

    def __cmp__(self, other):
        if isinstance(other, AdbDevice):
            return cmp(self.name, other.name)
        else:
            return cmp(self.name, other)

    def __str__(self):
        return 'AdbDevice({}, {})'.format(self.name, self.status)

    __repr__ = __str__


class ApkInfo(object):

    version_regex = re.compile(r"name='(?P<name>[^']+)' versionCode='(?P<vcode>[^']+)' versionName='(?P<vname>[^']+)'")
    name_regex = re.compile(r"name='(?P<name>[^']+)'")

    def __init__(self, path=None):
        self.path = path
        self.package = None
        self.activity = None
        self.label = None
        self.version_name = None
        self.version_code = None
        self.native_code = None
        self.parse(path)

    def parse(self, apk_path):
        _check_env()
        command = [aapt, 'dump', 'badging', apk_path]
        logger.debug(' '.join(command))
        output = subprocess.check_output(command)
        for line in output.split('\n'):
            if line.startswith('application-label:'):
                self.label = line.split(':')[1].strip().replace('\'', '')
            elif line.startswith('package:'):
                match = self.version_regex.search(line)
                if match:
                    self.package = match.group('name')
                    self.version_code = match.group('vcode')
                    self.version_name = match.group('vname')
            elif line.startswith('launchable-activity:'):
                match = self.name_regex.search(line)
                self.activity = match.group('name')
            elif line.startswith('native-code'):
                apk_abis = [entry.strip() for entry in line.split(':')[1].split("'") if entry.strip()]
                mapped_abis = []
                for apk_abi in apk_abis:
                    found = False
                    for abi, architectures in ABI_MAP.iteritems():
                        if apk_abi in architectures:
                            mapped_abis.append(abi)
                            found = True
                            break
                    if not found:
                        mapped_abis.append(apk_abi)
                self.native_code = mapped_abis
            else:
                pass  # not interested


class AdbConnection(object):

    # maintains the count of parallel active connections to a device, so that
    # adb disconnect is not invoked untill all connections are closed
    active_connections = defaultdict(int)
    default_timeout = 10
    ls_command = 'ls'

    @property
    def name(self):
        return self.device

    @property
    @memoized
    def newline_separator(self):
        output = adb_command(self.device,
                             "shell '({}); echo \"\n$?\"'".format(self.ls_command), adb_server=self.adb_server)
        if output.endswith('\r\n'):
            return '\r\n'
        elif output.endswith('\n'):
            return '\n'
        else:
            raise DevlibError("Unknown line ending")

    # Again, we need to handle boards where the default output format from ls is
    # single column *and* boards where the default output is multi-column.
    # We need to do this purely because the '-1' option causes errors on older
    # versions of the ls tool in Android pre-v7.
    def _setup_ls(self):
        command = "shell '(ls -1); echo \"\n$?\"'"
        try:
            output = adb_command(self.device, command, timeout=self.timeout, adb_server=self.adb_server)
        except subprocess.CalledProcessError as e:
            raise HostError(
                'Failed to set up ls command on Android device. Output:\n'
                + e.output)
        lines = output.splitlines()
        retval = lines[-1].strip()
        if int(retval) == 0:
            self.ls_command = 'ls -1'
        else:
            self.ls_command = 'ls'
        logger.debug("ls command is set to {}".format(self.ls_command))

    def __init__(self, device=None, timeout=None, platform=None, adb_server=None):
        self.timeout = timeout if timeout is not None else self.default_timeout
        if device is None:
            device = adb_get_device(timeout=timeout, adb_server=adb_server)
        self.device = device
        self.adb_server = adb_server
        adb_connect(self.device)
        AdbConnection.active_connections[self.device] += 1
        self._setup_ls()

    def push(self, source, dest, timeout=None):
        if timeout is None:
            timeout = self.timeout
        command = "push '{}' '{}'".format(source, dest)
        if not os.path.exists(source):
            raise HostError('No such file "{}"'.format(source))
        return adb_command(self.device, command, timeout=timeout, adb_server=self.adb_server)

    def pull(self, source, dest, timeout=None):
        if timeout is None:
            timeout = self.timeout
        # Pull all files matching a wildcard expression
        if os.path.isdir(dest) and \
           ('*' in source or '?' in source):
            command = 'shell {} {}'.format(self.ls_command, source)
            output = adb_command(self.device, command, timeout=timeout, adb_server=self.adb_server)
            for line in output.splitlines():
                command = "pull '{}' '{}'".format(line.strip(), dest)
                adb_command(self.device, command, timeout=timeout, adb_server=self.adb_server)
            return
        command = "pull '{}' '{}'".format(source, dest)
        return adb_command(self.device, command, timeout=timeout, adb_server=self.adb_server)

    def execute(self, command, timeout=None, check_exit_code=False,
                as_root=False, strip_colors=True):
        return adb_shell(self.device, command, timeout, check_exit_code,
                         as_root, self.newline_separator,adb_server=self.adb_server)

    def background(self, command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, as_root=False):
        return adb_background_shell(self.device, command, stdout, stderr, as_root)

    def close(self):
        AdbConnection.active_connections[self.device] -= 1
        if AdbConnection.active_connections[self.device] <= 0:
            adb_disconnect(self.device)
            del AdbConnection.active_connections[self.device]

    def cancel_running_command(self):
        # adbd multiplexes commands so that they don't interfer with each
        # other, so there is no need to explicitly cancel a running command
        # before the next one can be issued.
        pass


def fastboot_command(command, timeout=None, device=None):
    _check_env()
    target = '-s {}'.format(device) if device else ''
    full_command = 'fastboot {} {}'.format(target, command)
    logger.debug(full_command)
    output, _ = check_output(full_command, timeout, shell=True)
    return output


def fastboot_flash_partition(partition, path_to_image):
    command = 'flash {} {}'.format(partition, path_to_image)
    fastboot_command(command)


def adb_get_device(timeout=None, adb_server=None):
    """
    Returns the serial number of a connected android device.

    If there are more than one device connected to the machine, or it could not
    find any device connected, :class:`devlib.exceptions.HostError` is raised.
    """
    # TODO this is a hacky way to issue a adb command to all listed devices

    # The output of calling adb devices consists of a heading line then
    # a list of the devices sperated by new line
    # The last line is a blank new line. in otherwords, if there is a device found
    # then the output length is 2 + (1 for each device)
    start = time.time()
    while True:
        output = adb_command(None, "devices", adb_server=adb_server).splitlines()  # pylint: disable=E1103
        output_length = len(output)
        if output_length == 3:
            # output[1] is the 2nd line in the output which has the device name
            # Splitting the line by '\t' gives a list of two indexes, which has
            # device serial in 0 number and device type in 1.
            return output[1].split('\t')[0]
        elif output_length > 3:
            message = '{} Android devices found; either explicitly specify ' +\
                      'the device you want, or make sure only one is connected.'
            raise HostError(message.format(output_length - 2))
        else:
            if timeout < time.time() - start:
                raise HostError('No device is connected and available')
            time.sleep(1)


def adb_connect(device, timeout=None, attempts=MAX_ATTEMPTS):
    _check_env()
    # Connect is required only for ADB-over-IP
    if "." not in device:
        logger.debug('Device connected via USB, connect not required')
        return
    tries = 0
    output = None
    while tries <= attempts:
        tries += 1
        if device:
            command = 'adb connect {}'.format(device)
            logger.debug(command)
            output, _ = check_output(command, shell=True, timeout=timeout)
        if _ping(device):
            break
        time.sleep(10)
    else:  # did not connect to the device
        message = 'Could not connect to {}'.format(device or 'a device')
        if output:
            message += '; got: "{}"'.format(output)
        raise HostError(message)


def adb_disconnect(device):
    _check_env()
    if not device:
        return
    if ":" in device and device in adb_list_devices():
        command = "adb disconnect " + device
        logger.debug(command)
        retval = subprocess.call(command, stdout=open(os.devnull, 'wb'), shell=True)
        if retval:
            raise TargetError('"{}" returned {}'.format(command, retval))


def _ping(device):
    _check_env()
    device_string = ' -s {}'.format(device) if device else ''
    command = "adb{} shell \"ls / > /dev/null\"".format(device_string)
    logger.debug(command)
    result = subprocess.call(command, stderr=subprocess.PIPE, shell=True)
    if not result:
        return True
    else:
        return False


def adb_shell(device, command, timeout=None, check_exit_code=False,
              as_root=False, newline_separator='\r\n', adb_server=None):  # NOQA
    _check_env()
    if as_root:
        command = 'echo \'{}\' | su'.format(escape_single_quotes(command))
    device_part = []
    if adb_server:
        device_part = ['-H', adb_server]
    device_part += ['-s', device] if device else []

    # On older combinations of ADB/Android versions, the adb host command always
    # exits with 0 if it was able to run the command on the target, even if the
    # command failed (https://code.google.com/p/android/issues/detail?id=3254).
    # Homogenise this behaviour by running the command then echoing the exit
    # code.
    adb_shell_command = '({}); echo \"\n$?\"'.format(command)
    actual_command = ['adb'] + device_part + ['shell', adb_shell_command]
    logger.debug('adb {} shell {}'.format(' '.join(device_part), command))
    raw_output, error = check_output(actual_command, timeout, shell=False)
    if raw_output:
        try:
            output, exit_code, _ = raw_output.rsplit(newline_separator, 2)
        except ValueError:
            exit_code, _ = raw_output.rsplit(newline_separator, 1)
            output = ''
    else:  # raw_output is empty
        exit_code = '969696'  # just because
        output = ''

    if check_exit_code:
        exit_code = exit_code.strip()
        re_search = AM_START_ERROR.findall('{}\n{}'.format(output, error))
        if exit_code.isdigit():
            if int(exit_code):
                message = ('Got exit code {}\nfrom target command: {}\n'
                           'STDOUT: {}\nSTDERR: {}')
                raise TargetError(message.format(exit_code, command, output, error))
            elif re_search:
                message = 'Could not start activity; got the following:\n{}'
                raise TargetError(message.format(re_search[0]))
        else:  # not all digits
            if re_search:
                message = 'Could not start activity; got the following:\n{}'
                raise TargetError(message.format(re_search[0]))
            else:
                message = 'adb has returned early; did not get an exit code. '\
                          'Was kill-server invoked?\nOUTPUT:\n-----\n{}\n'\
                          '-----\nERROR:\n-----\n{}\n-----'
                raise TargetError(message.format(raw_output, error))

    return output


def adb_background_shell(device, command,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         as_root=False):
    """Runs the sepcified command in a subprocess, returning the the Popen object."""
    _check_env()
    if as_root:
        command = 'echo \'{}\' | su'.format(escape_single_quotes(command))
    device_string = ' -s {}'.format(device) if device else ''
    full_command = 'adb{} shell "{}"'.format(device_string, escape_double_quotes(command))
    logger.debug(full_command)
    return subprocess.Popen(full_command, stdout=stdout, stderr=stderr, shell=True)


def adb_list_devices(adb_server=None):
    output = adb_command(None, 'devices',adb_server=adb_server)
    devices = []
    for line in output.splitlines():
        parts = [p.strip() for p in line.split()]
        if len(parts) == 2:
            devices.append(AdbDevice(*parts))
    return devices


def adb_command(device, command, timeout=None,adb_server=None):
    _check_env()
    device_string = ""
    if adb_server != None:
        device_string = ' -H {}'.format(adb_server)
    device_string += ' -s {}'.format(device) if device else ''
    full_command = "adb{} {}".format(device_string, command)
    logger.debug(full_command)
    output, _ = check_output(full_command, timeout, shell=True)
    return output


# Messy environment initialisation stuff...

class _AndroidEnvironment(object):

    def __init__(self):
        self.android_home = None
        self.platform_tools = None
        self.adb = None
        self.aapt = None
        self.fastboot = None


def _initialize_with_android_home(env):
    logger.debug('Using ANDROID_HOME from the environment.')
    env.android_home = android_home
    env.platform_tools = os.path.join(android_home, 'platform-tools')
    os.environ['PATH'] = env.platform_tools + os.pathsep + os.environ['PATH']
    _init_common(env)
    return env


def _initialize_without_android_home(env):
    adb_full_path = which('adb')
    if adb_full_path:
        env.adb = 'adb'
    else:
        raise HostError('ANDROID_HOME is not set and adb is not in PATH. '
                        'Have you installed Android SDK?')
    logger.debug('Discovering ANDROID_HOME from adb path.')
    env.platform_tools = os.path.dirname(adb_full_path)
    env.android_home = os.path.dirname(env.platform_tools)
    _init_common(env)
    return env


def _init_common(env):
    logger.debug('ANDROID_HOME: {}'.format(env.android_home))
    build_tools_directory = os.path.join(env.android_home, 'build-tools')
    if not os.path.isdir(build_tools_directory):
        msg = '''ANDROID_HOME ({}) does not appear to have valid Android SDK install
                 (cannot find build-tools)'''
        raise HostError(msg.format(env.android_home))
    versions = os.listdir(build_tools_directory)
    for version in reversed(sorted(versions)):
        aapt_path = os.path.join(build_tools_directory, version, 'aapt')
        if os.path.isfile(aapt_path):
            logger.debug('Using aapt for version {}'.format(version))
            env.aapt = aapt_path
            break
    else:
        raise HostError('aapt not found. Please make sure at least one Android '
                        'platform is installed.')


def _check_env():
    global android_home, platform_tools, adb, aapt  # pylint: disable=W0603
    if not android_home:
        android_home = os.getenv('ANDROID_HOME')
        if android_home:
            _env = _initialize_with_android_home(_AndroidEnvironment())
        else:
            _env = _initialize_without_android_home(_AndroidEnvironment())
        android_home = _env.android_home
        platform_tools = _env.platform_tools
        adb = _env.adb
        aapt = _env.aapt

class LogcatMonitor(threading.Thread):

    FLUSH_SIZE = 1000

    @property
    def logfile(self):
        return self._logfile

    def __init__(self, target, regexps=None):
        super(LogcatMonitor, self).__init__()

        self.target = target

        self._stopped = threading.Event()
        self._match_found = threading.Event()

        self._sought = None
        self._found = None

        self._lines = Queue.Queue()
        self._datalock = threading.Lock()
        self._regexps = regexps

    def start(self, outfile=None):
        if outfile:
            self._logfile = outfile
        else:
            fd, self._logfile = tempfile.mkstemp()
            os.close(fd)
        logger.debug('Logging to {}'.format(self._logfile))

        super(LogcatMonitor, self).start()

    def run(self):
        self.target.clear_logcat()

        logcat_cmd = 'logcat'

        # Join all requested regexps with an 'or'
        if self._regexps:
            regexp = '{}'.format('|'.join(self._regexps))
            if len(self._regexps) > 1:
                regexp = '({})'.format(regexp)
            logcat_cmd = '{} -e "{}"'.format(logcat_cmd, regexp)

        logger.debug('logcat command ="{}"'.format(logcat_cmd))
        self._logcat = self.target.background(logcat_cmd)

        while not self._stopped.is_set():
            line = self._logcat.stdout.readline(1024)
            if line:
                self._add_line(line)

    def stop(self):
        # Kill the underlying logcat process
        # This will unblock self._logcat.stdout.readline()
        host.kill_children(self._logcat.pid)
        self._logcat.kill()

        self._stopped.set()
        self.join()

        self._flush_lines()

    def _add_line(self, line):
        self._lines.put(line)

        if self._sought and re.match(self._sought, line):
            self._found = line
            self._match_found.set()

        if self._lines.qsize() >= self.FLUSH_SIZE:
            self._flush_lines()

    def _flush_lines(self):
        with self._datalock:
            with open(self._logfile, 'a') as fh:
                while not self._lines.empty():
                    fh.write(self._lines.get())

    def clear_log(self):
        with self._datalock:
            while not self._lines.empty():
                self._lines.get()
                
            with open(self._logfile, 'w') as fh:
                pass

    def get_log(self):
        """
        Return the list of lines found by the monitor
        """
        self._flush_lines()

        with self._datalock:
            with open(self._logfile, 'r') as fh:
                res = [line for line in fh]

        return res

    def search(self, regexp):
        """
        Search a line that matches a regexp in the logcat log
        Return immediatly
        """
        res = []

        self._flush_lines()

        with self._datalock:
            with open(self._logfile, 'r') as fh:
                for line in fh:
                    if re.match(regexp, line):
                        res.append(line)

        return res

    def wait_for(self, regexp, timeout=30):
        """
        Search a line that matches a regexp in the logcat log
        Wait for it to appear if it's not found
        """
        res = self.search(regexp)

        # Found some matches, return them
        # Also return if thread not running
        if len(res) > 0 or not self.is_alive():
            return res

        # Did not find any match, wait for one to pop up
        self._sought = regexp
        found = self._match_found.wait(timeout)
        self._match_found.clear()
        self._sought = None

        if found:
            return [self._found]
        else:
            raise RuntimeError('Logcat monitor timeout ({}s)'.format(timeout))

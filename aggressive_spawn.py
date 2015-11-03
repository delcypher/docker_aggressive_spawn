#!/usr/bin/python3
# vim: set sw=2 ts=2 softtabstop=2 expandtab:
"""
Script to aggressively create, run and destroy containers.
This script was supposed to create the conditions
necessary to observe https://github.com/docker/docker/issues/14203

Cannot start container <CONTAINER_ID>: [8] System error: read parent: connection reset by peer

However instead it seems to create the conditions necessary to trigger.
https://github.com/docker/docker/issues/14474

Cannot destroy container e1a8afd2d2a655cdbc75a5180ca01a4370be28f391189a29366923307deb0cb7: Driver overlay failed to remove init filesystem e1a8afd2d2a655cdbc75a5180ca01a4370be28f391189a293669233
07deb0cb7-init: remove /home/user/docker/overlay/e1a8afd2d2a655cdbc75a5180ca01a4370be28f391189a29366923307deb0cb7-init/merged: device or resource busy'
"""
import argparse
import concurrent.futures
import logging
import os
import random
import signal
import sys
import threading

# Need to be global
_logger = None
futureToRunner = None

def handleInterrupt(signum, frame):
  logging.info('Received signal {}'.format(signum))
  if futureToRunner != None:
    cancel(futureToRunner)

def cancel(futureToRunner):
  _logger.warning('Cancelling futures')
  # Cancel all futures first. If we tried
  # to kill the runner at the same time then
  # other futures would start which we don't want
  for future in futureToRunner.keys():
    future.cancel()
  # Then we can kill the runners if required
  for runner in futureToRunner.values():
    runner.kill()

try:
  import docker
except ImportError:
  print("Could not import docker module from docker-py. Did you install it? See requirements.txt")
  raise

class RunnerJob:
  def __init__(self, id, imageID):
    _logger.debug('Creating RunnerJob {}'.format(id))
    self.id = id
    self.imageID = imageID
    self.killLock = threading.Lock()
    self.container = None
    try:
      self.dc = docker.Client()
      self.dc.ping()
    except Exception as e:
      _logger.error('Failed to connect to the Docker daemon')
      raise

  def run(self):
    _logger.info('Running {}'.format(self.id))
    
    # Configuration
    hostCfg = self.dc.create_host_config(
      privileged=False,
      network_mode=None,
      mem_limit='128m',
      memswap_limit='128m',
      ulimits = [ docker.utils.Ulimit(name='stack', soft=(128 * 2**20), hard=(128 * 2**20)) ]
    )
    # Create container
    self.container = self.dc.create_container(
      image=self.imageID,
      command=['sleep', '{:.3}'.format(random.uniform(0,0.5))],
      host_config=hostCfg
    )
    self.container = self.container['Id']
    try:
      self.dc.start(container=self.container)
      self.dc.wait(container=self.container)
    except Exception as e:
      _logger.error('Exception raised whilst running running container:\n{}'.format(str(e)))
      raise
    finally:
      self.kill()

  def kill(self):
    try:
      # Lock execution of this method to prevent races
      self.killLock.acquire()
      _logger.info('Trying to kill {}'.format(self.id))

      if self.container == None:
        return

      # Try to kill container if it is running
      try:
        containerStatus = self.dc.inspect_container(self.container)
        if containerStatus["State"]["Running"]:
          self.dc.kill(self.container)
      except docker.errors.APIError as e:
        # This might fail as it is susceptible to a TOCTOU race
        pass
      # Destroy the container
      try:
        _logger.info('Destroying container:{}'.format(self.container))
        self.dc.remove_container(container=self.container, force=True)
      except docker.errors.APIError as e:
        _logger.error('Failed to remove container:"{}".\n{}'.format(self.container, str(e)))
      # Make sure any future calls to kill() do nothing
      self.container = None
    finally:
      self.dc.close() # We're done with the client, close it
      self.killLock.release()


def main(args):
  global _logger, futureToRunner
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument("-l","--log-level",type=str, default="info", dest="log_level", choices=['debug','info','warning','error'])
  parser.add_argument("-j","--jobs", type=int, default=1, help='Number of jobs to run in parallel')
  parser.add_argument("count", type=int, help='Number of containers to spawn')
  parser.add_argument("docker_image_id", type=str, help='Docker image ID')

  pargs = parser.parse_args()

  logLevel = getattr(logging, pargs.log_level.upper(),None)
  if logLevel == logging.DEBUG:
    logFormat = '%(levelname)s:%(threadName)s: %(filename)s:%(lineno)d %(funcName)s()  : %(message)s'
  else:
    logFormat = '%(levelname)s:%(threadName)s: %(message)s'

  logging.basicConfig(level=logLevel, format=logFormat)
  _logger = logging.getLogger(__name__)

  if pargs.count < 1:
    _logger.error('count must be >= 1')
    return 1

  random.seed()

  _logger.info('Creating {} jobs'.format(pargs.count))
  jobs = []
  for id in range(0 ,pargs.count):
    jobs.append(RunnerJob(id, pargs.docker_image_id))

  # Run jobs in parallel
  # Signal handlers are to handle being killed gracefully
  _logger.info('Starting jobs')
  signal.signal(signal.SIGINT, handleInterrupt)
  signal.signal(signal.SIGTERM, handleInterrupt)
  try:
    with concurrent.futures.ThreadPoolExecutor(max_workers=pargs.jobs) as executor:
      futureToRunner = { executor.submit(r.run) : r for r in jobs }
      for future in concurrent.futures.as_completed(futureToRunner):
        r = futureToRunner[future]
        try:
          if future.exception():
            _logger.error('Job {} raised exception:\n{}'.format(r.id, future.exception()))
          else:
            _logger.debug('Job {} completed without error'.format(r.id))
        except concurrent.futures.CancelledError as e:
          _logger.error('Job {} was cancelled'.format(r.id))
  except KeyboardInterrupt:
    _logger.error('keyboard interrupt')
  finally:
    # Switch back to default signal handlers
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)

  return 0

if __name__ == '__main__':
  sys.exit(main(sys.argv[1:]))

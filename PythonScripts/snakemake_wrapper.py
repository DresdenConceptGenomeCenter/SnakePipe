#!/usr/bin/env python3
'''
The MIT License (MIT)

Copyright (c) <2018> <DresdenConceptGenomeCenter>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

Use Python Naming Conventions
https://www.python.org/dev/peps/pep-0008/#naming-conventions

contact: mathias.lesche(at)tu-dresden.de
'''

''' python modules '''
import logging

from argparse import ArgumentParser as ArgumentParser
from argparse import RawDescriptionHelpFormatter

from datetime import datetime

from os import listdir

from os.path import isfile
from os.path import join as pathjoin

from re import search

from subprocess import Popen
from subprocess import PIPE
from subprocess import CalledProcessError
from subprocess import run

from sys import argv as argv

''' own modules '''
from helper.io_module import check_file
from helper.io_module import create_directory

from helper.helper_logger import MainLogger

from snake_information import SnakeInformation


class Parser(object):
    def __init__(self):
        self.__parser = ArgumentParser(description="""
        Wrapper function for the snakemake pipeline
        """, formatter_class=RawDescriptionHelpFormatter, 
#         add_help = False, prog = 'snakemake_wrapper.py')
        add_help = True, prog = 'snakemake_wrapper.py')
        
        self.initialiseParser()
        self.__drmaa = '--drmaa'
        self.__dryrun = '--printshellcmds --dryrun'
        self.__configfile = '--configfile'
        self.__snakefile = '--snakefile'
        self.__workflow = ''
        self.__clusterconfig = '--cluster-config'
        self.__clusterlog = 'snakemake_logs'
        self.__scheduler = ''
        self.__jobs = '--jobs'
        self.__cmd = False
        
        self.__z_scheduler = ''
        self.__z_flows = ''
        self.__zl_flows = False

        self.__c_scheduler = ''
        self.__c_flows = ''
        self.__cl_flows = False

        self.__snakelist = ['--local-cores 1']
        self.__sublist = []
        
        self.__subcmd = 'snakemake'

        self.__logger = logging.getLogger('snakemake.wrapper')
        self.parse()

    def initialiseParser(self):
#         self.__parser.add_argument('-h', '--help', dest = 'h', help = 'Display help message', action= 'store_true')
#         self.__parser.add_argument('-w', '--job-scheduler', dest='scheduler', metavar='STRING', choices = ('qsub', 'sbatch', 'drmaa', 'local'), required = True, help = 'job scheduler system (qsub, sbatch, drmaa, local)')
#         self.__parser.add_argument('-f', '--workflow', dest = 'workflow', help = 'select a snakemake workflow')
        self.__parser.add_argument('-p', '--print', dest='cmd', help = 'print command and exit', action= 'store_true')
        self.__parser.add_argument('-l', '--log-dir', dest='clusterlog', metavar='DIRECTORY', type = str, default = 'snakemake_logs', help = 'log directory for snakemake (default: snakemake_logs)')
        

        self.__wrapper = self.__parser.add_argument_group("snakemake specific arguments")
        self.__wrapper.add_argument('-n', '--dryrun', dest = 'dryrun', action = 'store_true', help = 'do not execute anything, and display what would be done')
        self.__wrapper.add_argument('-c', '--configfile', dest = 'configfile', metavar='FILE', type = str, help = 'configuration file needed to run the workflow')
        self.__wrapper.add_argument('-u', '--cluster-config', dest = 'clusterconfig', metavar='FILE', type = str, help = 'configuration file to specify required cluster resources')
        self.__wrapper.add_argument('-s', '--snakefile', dest = 'snakefile', metavar='FILE', type = str, help = 'the workflow definition in a snakefile')
        self.__wrapper.add_argument('-j', '--jobs', dest = 'jobs', metavar='INT', type = int, default = 1, help = 'run at most N jobs in parallel (default: 1)')
        
        self.__subparser = self.__parser.add_subparsers(title= 'Snankemake workflows', description='Snakemake workflows', help='list workflows for snakemake pipeline')
        self.__cmcbworkflow = self.__subparser.add_parser('CMCB')
        self.__cmcbworkflow.add_argument('-w', '--job-scheduler', dest = 'c_scheduler', metavar = 'STRING', choices = ('qsub', 'drmaa', 'local'), help = "job scheduler system ('qsub', 'drmaa', 'local')")
        self.__cmcbworkflow.add_argument('-f', '--workflow', dest = 'c_flows', metavar = 'STRING', help = 'choose workflow')
        self.__cmcbworkflow.add_argument('-l', '--list-workflows', dest = 'cl_flows', action = 'store_true', help = 'list workflows')
 
        self.__zihworkflow = self.__subparser.add_parser('ZIH')
        self.__zihworkflow.add_argument('-w', '--job-scheduler', dest = 'z_scheduler', metavar = 'STRING', choices = ('sbatch', 'drmaa', 'local'), help = "job scheduler system ('sbatch', 'drmaa', 'local')")
        self.__zihworkflow.add_argument('-f', '--workflow', dest = 'z_flows', metavar = 'STRING', help = 'choose workflow')
        self.__zihworkflow.add_argument('-l', '--list-workflows', dest = 'zl_flows', action = 'store_true', help = 'list workflows')
        

    def parse(self, inputstring = None):
        if inputstring == None:
            self.__options = self.__parser.parse_args()
        else:
            self.__options = self.__parser.parse_args(inputstring)

    def show_log(self, level, message):
        if level == 'debug':
            self.__logger.debug(message)
        elif level == 'info':
            self.__logger.info(message)
        elif level == 'warning':
            self.__logger.warning(message)
        elif level == 'error':
            self.__logger.error(message)
        elif level == 'critical':
            self.__logger.critical(message)


#     def show_workflow(self):
#         if self.

    '''
    function retrieves the available workflows from a give directory
    @param directory: string
    @return: list 
    '''
    def get_workflow_names(self, directory):
        tempflows = [i for i in listdir(directory) if isfile(pathjoin(directory, i))]
        tempflows = [i.replace('.snakemake', '').replace('do_', '') for i in tempflows if i.startswith('do')]
        return tempflows

    '''
    function checks the CMCB specific options. It can show the available snake workflows, check if the selected snake
    workflow is valid, set the job scheduler and set the cluster config file. However, the configfile has to be provided
    by the '-c'/'--configfile parameter
    '''
    def parse_cmcb(self):
#         find cmcb workflows and print them
        if self.__options.cl_flows:
            snakeflows = self.get_workflow_names(SnakeInformation.SNAKE_CMCB_WORKFLOW)
            if len(snakeflows) >= 1:
                self.show_log('info', 'Workflows for CMCB:')
                for i in snakeflows:
                    self.show_log('info', 'CMCB: {0}'.format(i.replace('.snakemake', '')))
                exit(0)
            else:
                self.show_log('error', 'No workflow available for CMCB')
                exit(2)

#         cmcb entered but no attributes, print help
        if self.__options.c_flows is None and self.__options.c_scheduler is None:
            self.__parser.parse_args(['CMCB', '--help'])
            exit(0)
            
#         select cmcb workflows check for match with the provided workflow
        if self.__options.c_flows is not None:
            snakeflows = self.get_workflow_names(SnakeInformation.SNAKE_CMCB_WORKFLOW)
            if self.__options.c_flows not in snakeflows:
                self.show_log('error', "-f/--workflow: '{0}' provide valid snakemake workflow".format(self.__options.c_flows))
                exit(2)
            else:
                self.__workflow = self.__options.c_flows
                self.__snakefile = '{0} {1}'.format(self.__snakefile, pathjoin(SnakeInformation.SNAKE_CMCB_WORKFLOW, 'do_{0}.snakemake'.format(self.__workflow)))
                self.__snakelist.append(self.__snakefile)
#                 check if cluster config was given, if not set to default
                self.check_set_clusterconfig('CMCB')

        if self.__options.c_scheduler is not None:
            self.__scheduler = self.__options.c_scheduler
        else:
            self.show_log('error', "-w/--job-schedler: {0} provide valid scheduler ('qsub', 'drmaa', 'local')".format(self.__options.c_flows))
            exit(2)

    '''
    function checks the ZIH specific options. It can show the available snake workflows, check if the selected snake
    workflow is valid, set the job scheduler and set the cluster config file. However, the configfile has to be provided
    by the '-c'/'--configfile parameter
    '''
    def parse_zih(self):
#         find cmcb workflows and print them
        if self.__options.zl_flows:
            snakeflows = self.get_workflow_names(SnakeInformation.SNAKE_ZIH_WORKFLOW)
            if len(snakeflows) >= 1:
                self.show_log('info', 'Workflows for ZIH:')
                for i in snakeflows:
                    self.show_log('info', 'ZIH: {0}'.format(i.replace('.snakemake', '')))
                exit(0)
            else:
                self.show_log('error', 'No workflow available for ZIH')
                exit(2)

#         cmcb entered but no attributes, print help
        if self.__options.z_flows is None and self.__options.z_scheduler is None:
            self.__parser.parse_args(['ZIH', '--help'])
            exit(0)
            
#         select cmcb workflows check for match with the provided workflow
        if self.__options.z_flows is not None:
            snakeflows = self.get_workflow_names(SnakeInformation.SNAKE_ZIH_WORKFLOW)
            if self.__options.z_flows not in snakeflows:
                self.show_log('error', "-f/--workflow: '{0}' provide valid snakemake workflow".format(self.__options.z_flows))
                exit(2)
            else:
                self.__workflow = self.__options.z_flows
                self.__snakefile = '{0} {1}'.format(self.__snakefile, pathjoin(SnakeInformation.SNAKE_ZIH_WORKFLOW, '{0}.snakemake'.format(self.__workflow)))
                self.__snakelist.append(self.__snakefile)
#                 check if cluster config was given, if not set to default
                self.check_set_clusterconfig('CMCB')

        if self.__options.z_scheduler is not None:
            self.__scheduler = self.__options.z_scheduler
        else:
            self.show_log('error', "-w/--job-schedler: {0} provide valid scheduler ('sbatch', 'drmaa', 'local')".format(self.__options.z_flows))
            exit(2)

    '''
    function checks if the cluster config was provided. if it is, a simple file check is done.
    one can provide a subystem too and then it's set to the default value
    @param subsytem: string
    
    '''
    def check_set_clusterconfig(self, subsystem = ''):
        # check if clusterconfig is a valid file and add to clusterconfig option
        if self.__options.clusterconfig is not None:
            tempfile = check_file(self.__options.clusterconfig)
            if len(tempfile) == 0:
                self.show_log('error', '-u/--cluster-config: {0} provide valid cluster-config file'.format(self.__options.clusterconfig))
                exit(2)
            else:
                self.__clusterconfig = '{0} {1}'.format(self.__clusterconfig, tempfile)
                self.__snakelist.append(self.__clusterconfig)
#         if non was given check for the submission system and set to default
        elif subsystem == 'CMCB':
            self.__clusterconfig = '{0} {1}'.format(self.__clusterconfig, SnakeInformation.SNAKE_CMCB_CONFIG)
            self.__snakelist.append(self.__clusterconfig)
        elif subsystem == 'ZIH':
            self.__clusterconfig = '{0} {1}'.format(self.__clusterconfig, SnakeInformation.SNAKE_ZIH_CONFIG)
            self.__snakelist.append(self.__clusterconfig)
        
#         when no file is provided, error and exit
        if self.__clusterconfig == '--cluster-config':
            self.show_log('error', '-u/--cluster-config: provide valid cluster-config file')
            exit(2)

    '''
    function checks if a configfile was provided
    '''
    def check_set_configfile(self):
        # check if configfile is a valid file and add to configfile option
        if self.__options.configfile is not None:
            tempfile = check_file(self.__options.configfile)
            if len(tempfile) == 0:
                self.show_log('error', '-c/--configfile: {0} provide valid config file'.format(self.__options.configfile))
                exit(2)
            else:
                self.__configfile = '{0} {1}'.format(self.__configfile, tempfile)
                self.__snakelist.append(self.__configfile)
        else:
            self.show_log('error', '-c/--configfile: provide valid config file'.format(self.__options.configfile))
            exit(2)

    '''
    functinos checks if a snakefile was provided
    '''
    def check_set_snakefile(self):
        # check if snakefile is a valid file and add to snakefile option
        if self.__options.snakefile is not None:
            tempfile = check_file(self.__options.snakefile)
            if len(tempfile) == 0:
                self.show_log('error', '-s/--snakefile: {0} provide valid snakefile'.format(self.__options.snakefile))
                exit(2)
            else:
                self.__snakefile = '{0} {1}'.format(self.__snakefile, tempfile)
                self.__snakelist.append(self.__snakefile)
        

        if self.__snakefile == '--snakefile':
            self.show_log('error', '-s/--snakefile: provide valid snakefile'.format(self.__options.snakefile))
            exit(2)
    
    '''
    function checks if the job argument is set correctly. otherwise it defaults to 1
    '''
    def check_set_jobs(self):
        if isinstance(self.__options.jobs, int):
            self.__jobs = '{0} {1}'.format(self.__jobs, self.__options.jobs)
            self.__snakelist.append(self.__jobs)
        else:
            self.show_log('error', '-j/--jobs: {0} provide an integer'.format(self.__options.jobs))
            exit(2)

    def main(self):
        # no attributes, print help
        if len(argv) < 2:
            self.__parser.print_help()
            exit(0)
        
        self.__cmd = self.__options.cmd

        if self.__options.dryrun: self.__snakelist.append(self.__dryrun)
        
        self.check_set_jobs()

        print(self.__options)
        if 'cl_flows' in self.__options:
            self.parse_cmcb()
        elif 'zl_flows' in self.__options:
            self.parse_zih()
        
        self.check_set_clusterconfig() # check the cluster config file
        self.check_set_configfile() # check the config file (usually yaml)
        self.check_set_snakefile() # check if snakefile is there

        self.__clusterlog = self.__options.clusterlog
        
    
    def prepare_run(self):      
        self.__subcmd = '{0} {1}'.format(self.__subcmd, ' '.join(self.__snakelist))

        if self.__scheduler == 'drmaa':
            pass
        elif self.__scheduler == 'qsub':
            outputdir =  '-o {0}'.format(self.__clusterlog)
            errordir = '-e {0}'.format(self.__clusterlog)
            self.__subcmd = '{0} --cluster "qsub -b -n {1} {2} -S {{cluster.shell}} -q {{cluster.queue}} -l h_rt={{cluster.runtime}} -l mem_free={{cluster.memory}} -l {{cluster.other_resources}} -pe smp {{cluster.cpu}}"'.format(self.__subcmd, outputdir, errordir)
        
        elif self.__scheduler == 'local':
            pass
        else:
            self.__clusterlog = pathjoin(self.__clusterlog, '{0}_{1}'.format(self.__clusterlog, datetime.now().strftime('%y%m%d-%H%M')))
            outputfile = pathjoin(self.__clusterlog, 'slurm-%j.out')
            errorfile = pathjoin(self.__clusterlog, 'slurm-%j.err')
            self.__subcmd = '{0} --cluster "sbatch --output={1} --error={2} --time={{cluster.runtime}} --mem-per-cpu={{cluster.memory}} --cpus-per-task {{cluster.cpu}}"'.format(self.__subcmd, outputfile, errorfile)
        
        if self.__cmd:
            self.show_log('info', 'COMMAND: {0}'.format(self.__subcmd))
            exit(0)

#         create the log directory
        create_directory(self.__clusterlog)

        try:
            res = run(self.__subcmd, check=True, shell=True, stdout= PIPE, stderr = PIPE)
#             res = Popen(self.__subcmd, stdout= PIPE, stderr= PIPE)
        except CalledProcessError as e:
            raise e
    
        sterr = res.stderr.decode()
        stdout = res.stdout.decode()
    
        try:
            m = search("Submitted batch job (\d+)", sterr)
            jobid = m.group(1)
        except AttributeError:
            print(stdout)
            self.show_log('info', 'COMMAND: {0}'.format(self.__subcmd))
        except Exception as e:
            print(e)
            raise


if __name__ == '__main__':
    mainlog = MainLogger('snakemake')
    parser = Parser()
    parser.main()
    parser.prepare_run()

    mainlog.close()
    logging.shutdown()

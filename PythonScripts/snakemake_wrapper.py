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

from re import search

from subprocess import Popen
from subprocess import PIPE
from subprocess import CalledProcessError
from subprocess import run



''' own modules '''
from helper.io_module import check_file

from helper.helper_logger import MainLogger


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
        self.__clusterconfig = '--cluster-config'
        self.__scheduler = ''
        self.__jobs = '--jobs'
        self.__cmd = False

        self.__snakelist = ['--local-cores 1']
        self.__sublist = []
        
        self.__subcmd = 'snakemake'

        self.__logger = logging.getLogger('snakemake.wrapper')
        self.parse()

    def initialiseParser(self):
#         self.__parser.add_argument('-h', '--help', dest = 'h', help = 'Display help message', action= 'store_true')
#         self.__parser.add_argument('-w', '--job-scheduler', dest='scheduler', metavar='STRING', choices = ('qsub', 'sbatch', 'drmaa', 'local'), required = True, help = 'job scheduler system (qsub, sbatch, drmaa, local)')
        self.__parser.add_argument('-p', '--print', dest='cmd', help = 'print command and exit', action= 'store_true')
#         self.__parser.add_argument('-f', '--workflow', dest = 'workflow', help = 'select a snakemake workflow')
        
        
        self.__wrapper = self.__parser.add_argument_group("snakemake specific arguments")
        self.__wrapper.add_argument('-n', '--dryrun', dest = 'dryrun', action = 'store_true', help = 'do not execute anything, and display what would be done')
        self.__wrapper.add_argument('-c', '--configfile', dest = 'configfile', metavar='FILE', type = str, help = 'configuration file needed to run the workflow')
        self.__wrapper.add_argument('-u', '--cluster-config', dest = 'clusterconfig', metavar='FILE', type = str, help = 'configuration file to specify required cluster resources')
        self.__wrapper.add_argument('-s', '--snakefile', dest = 'snakefile', metavar='FILE', type = str, help = 'the workflow definition in a snakefile')
        self.__wrapper.add_argument('-j', '--jobs', dest = 'jobs', metavar='INT', type = int, default = 1, help = 'run at most N jobs in parallel (default: 1)')
        
        self.__subparser = self.__parser.add_subparsers(help='sub-command help')
        
        self.__cmcbworkflow = self.__subparser.add_subparsers(title='CMCB workflows', description='snakemake workflows at CMCB', help='list workflows for snakemake pipeline at CMCB')
        self.__cmcbworkflow.add_argument('-w', '--job-scheduler', dest = 'scheduler', metavar = 'STRING', choices = ('qsub', 'drmaa', 'local'), help = 'job scheduler system (sbatch, drmaa, local)')
        self.__cmcbworkflow.add_argument('-f', '--flows', dest = 'flows', metavar = 'STRING', help = 'list workflows')

        self.__zihworkflow = self.__subparser.add_subparsers(title='ZIH workflows', description='snakemake workflows at ZIH', help='list workflows for snakemake pipeline at ZIH')
        self.__zihworkflow.add_argument('-w', '--job-scheduler', dest = 'scheduler', metavar = 'STRING', choices = ('sbatch', 'drmaa', 'local'), help = 'job scheduler system (sbatch, drmaa, local)')
        self.__zihworkflow.add_argument('-f', '--flows', dest = 'flows', metavar = 'STRING', help = 'list workflows')
        
        
#         --cluster-config FILE, -u FILE
        
        
    #     self.__parser.add_argument('-f', '--files', type=str, metavar='FILE', dest='file', nargs= '+', help="list of fq-files(' ' separated)")
        #self.__parser.add_argument('-o', '--output', type=str, metavar='STRING', dest='output', required=True, help='id of the run which will be used as output folder')
        #self.__parser.add_argument('-u', '--undet', dest='undet', action='store_true', help='transfer undetermined')
        #self.__parser.add_argument("-e", "--email", dest = 'email', action= 'store_true', help= 'activates email notification')
        #self.__parser.add_argument('-q', '--qstat', type=str, metavar='STRING', dest='qstatname', default='tsp_fq', help='qstat name of the qsub job (default: tsp_fq)')

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


    def main(self):
        #if self.__options.h:
        #    self.__parser.print_help()
        #    exit(0)
        
        self.__scheduler = self.__options.scheduler
        self.__cmd = self.__options.cmd
        
#         if self.__options.use_drmaa: self.__sublist.append(self.__drmaa)
        if self.__options.dryrun: self.__snakelist.append(self.__dryrun)
        
        if isinstance(self.__options.jobs, int):
            self.__jobs = '{0} {1}'.format(self.__jobs, self.__options.jobs)
            self.__snakelist.append(self.__jobs)
        else:
            self.show_log('error', '--jobs: {0} provide an integer'.format(self.__options.jobs))
        
        # check if configfile is a valid file and add to configfile option
        if self.__options.configfile is not None:
            tempfile = check_file(self.__options.configfile)
            if len(tempfile) == 0:
                self.show_log('error', '--configfile: {0} provide valid file'.format(self.__options.configfile))
                exit(2)
            else:
                self.__configfile = '{0} {1}'.format(self.__configfile, tempfile)
                self.__snakelist.append(self.__configfile)
        else:
            # go to default value
            pass

        
        # check if clusterconfig is a valid file and add to clusterconfig option
        if self.__options.clusterconfig is not None:
            tempfile = check_file(self.__options.clusterconfig)
            if len(tempfile) == 0:
                self.show_log('error', '--cluster-config: {0} provide valid file'.format(self.__options.clusterconfig))
                exit(2)
            else:
                self.__clusterconfig = '{0} {1}'.format(self.__clusterconfig, tempfile)
                self.__snakelist.append(self.__clusterconfig)
        else:
            # go to default value
            pass


        # check if snakefile is a valid file and add to snakefile option
        if self.__options.snakefile is not None:
            tempfile = check_file(self.__options.snakefile)
            if len(tempfile) == 0:
                self.show_log('error', '--snakefile: {0} provide valid file'.format(self.__options.snakefile))
                exit(2)
            else:
                self.__snakefile = '{0} {1}'.format(self.__snakefile, tempfile)
                self.__snakelist.append(self.__snakefile)
        else:
            # go to default value
            pass

    
    def prepare_run(self):      
        self.__subcmd = '{0} {1}'.format(self.__subcmd, ' '.join(self.__snakelist))

        if self.__scheduler == 'drmaa':
            pass
        elif self.__scheduler == 'qsub':
            pass
        else:
            self.__subcmd = '{0} --cluster "sbatch --output=slurm-%j.out --error=slurm-%j.err --time={{cluster.runtime}} --mem-per-cpu={{cluster.memory}} --cpus-per-task {{cluster.cpu}}"'.format(self.__subcmd)
        
        if self.__cmd:
            self.show_log('info', 'COMMAND: {0}'.format(self.__subcmd))
            exit(0)
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

'''Python module for simplifying the creation of a Command Line 
Interface (CLI) for a nipype workflow.'''

import os, shutil, argparse, imp, sys
from os import path
from hashlib import md5

#Try importing the local site configuration
clip_conf_path = os.getenv('CLIP_CONF')
if not (clip_conf_path is None or clip_conf_path == ''):
    clip_conf = imp.load_source('clip_conf', clip_conf_path)
    has_conf = True
else:
    has_conf = False

def get_working_dir_name(prog_name, base_in_hash, suffix=''):
    '''Get the working directory name. Will be unique for every 
    combination of username, program name, destination dir, and 
    (optionally) suffix.
    
    Parameters
    ----------
    prog_name : str
        The name of the command line interface program.
        
    dest_dir : str
        The destination directory for the program.
        
    suffix : str
        A suffix to append the the directory name. Allows collisions to 
        be avoided when the same program is being run multiple times by 
        the same user and with the same destination directory.
    '''
    return ('_%s_%s_%s_%s' % 
            (prog_name, 
             os.getenv('USER'), 
             base_in_hash.hexdigest()[:8], 
             suffix))
            
def get_common_parent(paths):
    '''Return the closest common ancestor directory for the provided 
    paths.
    
    Parameters
    ----------
    paths : iterable
        The paths to find the common ancestor of.
    '''
    str_prefix = path.commonprefix(paths)
    prefix_len = len(str_prefix)
    if (path.exists(str_prefix) and 
        path.isdir(str_prefix) and 
        all(len(pth) > prefix_len and pth[prefix_len] == os.sep
            for pth in paths)
       ):
        return str_prefix
    else:
        return path.split(str_prefix)[0]

non_dist_plugins = ('Debug', 'Linear', 'MultiProc')
'''Plugins that are definately not run in a distributed manner, and 
thus are not affected with file system synchronization issues.'''

class ResourceRequest(object):
    def __init__(self, time=None, mem=None, vmem=None, 
                 use_mpi=False, min_cores=1, max_cores=None):
        '''A generic resource request for a job on a cluster.
        
        Parameters
        ----------
        time : int or None
            Max time in seconds it will take this job to run
            
        mem : int or None
            Max memory in bytes
        
        vmem : int or None
            Max virtual memory in bytes
        
        use_mpi : bool
            Use MPI if available
                    
        min_cores : int
            The minimum number of cores to request if MPI or SMP 
            processing is available
            
        max_cores : int
            The maximum number of cores to request if MPI or SMP 
            processing is available
        '''
        self.time = time
        self.mem = mem
        self.vmem = vmem
        self.use_mpi = use_mpi
        self.min_cores = min_cores
        self.max_cores = max_cores
        
def get_full_plugin_args(exec_plugin, resource_request):
    '''Get the plugin args for the given exec_plugin and 
    resource_request. Gets this information from the local site 
    configuration.
    '''
    if not has_conf:
        return ''
    
    result = []
    if (hasattr(clip_conf, 'default_plugin_args') and 
        exec_plugin in clip_conf.default_plugin_args
       ):
        result.append(clip_conf.default_plugin_args[exec_plugin])
    if hasattr(clip_conf, 'get_plugin_args'):
        result.append(clip_conf.get_plugin_args(exec_plugin, 
                                                resource_request)
                     )
    return ' '.join(result)
        
class PypeCli(object):
    def __init__(self, arg_parser, base_input_opts, def_dest_opts=None):
        '''Create the command line interface to the pipeline.
        
        Parameters
        ----------
        arg_parser : argparse.ArgumentParser
            Should already be setup with any arguments needed by the 
            pipeline itself.
            
        base_input_opts : list
            List of options that would require the full pipeline to be 
            rerun if changed. A hash of these values is used when 
            determining the working directory name.
            
        def_dest_opts : list or None
            Any options that provide paths to use for defining the 
            default destination directory. The closest parent directory 
            of all the given paths will be used. If None is given, the 
            current working directory will be used. 
        '''
        self.arg_parser = arg_parser
        self.base_input_opts = base_input_opts
        self.def_dest_opts = def_dest_opts
        
        #Create appropriate help text about the default destination 
        #directory
        dest_help = ["The directory to store results under."]
        if def_dest_opts is None:
            dest_help.append("The default is the current working "
                             "directory.")
        else:
            dest_help.append("Defaults to the parent directory of")
            if len(def_dest_opts) == 1:
                dest_help.append("the %s" % def_dest_opts[0])
            else:
                dest_help.append("the paths given for: " % 
                                 def_dest_opts)
        
        gen_opt = arg_parser.add_argument_group('General', 
                                                description=\
                                                "Options applicable to any "
                                                "pipeline")
        gen_opt.add_argument('--dest-dir', default=None,
                             help=" ".join(dest_help)
                            )
        gen_opt.add_argument('--wd-root', default=None,
                             help="The directory to put the working "
                             "directory under. Default: %(default)s"
                            )
        gen_opt.add_argument('--wd-suffix', default='', 
                             help="Suffix to append to the pipeline "
                             "working directory name. Can be used to "
                             "prevent collisions between multiple "
                             "simultaneous runs with the same "
                             "base inputs."
                            )
        gen_opt.add_argument('--keep-wd', action='store_true',
                             help=("Don't delete working dir, even if "
                             "no errors ocurred.")
                            )
        gen_opt.add_argument('--exec-plugin',
                             help="Execution plugin to run the "
                             "pipeline with. For a description of "
                             "available plugins see: "
                             " http://nipy.org/nipype/users/plugins.html"
                             " , default: %(default)s")
        
        if has_conf and hasattr(clip_conf, 'cli_defaults'):
            arg_parser.set_defaults(**clip_conf.cli_defaults)
        
    def parse_args(self, argv):
        '''Parse the CLI arguments.
        
        Parameters
        ----------
        argv : list
            The sys.argv list.
            
        Returns
        -------
        args : namespace
            The results from ArgumentParser.parse_args
            
        dest : str
            The destination to store results under
            
        default_plugin_args : str
            The default arguments to the execution plugin. Needed for 
            any nodes that are going to override 'plugin_args'.
        '''
        self.prog_name = path.split(argv[0])[1]
        args = self.arg_parser.parse_args(argv[1:])
        
        #Get a hash of the "base" inputs
        base_inputs = []
        for opt in self.base_input_opts:
            if hasattr(args, opt):
                base_inputs.append(getattr(args, opt))
        base_in_hash = md5(''.join(str(x) for x in base_inputs))
        
        #Figure out the destination directory
        if args.dest_dir:
            dest_dir = path.abspath(args.dest_dir)
        elif self.def_dest_opts is None:
            dest_dir = os.getcwd()
        else:
            src_paths = []
            for opt in self.def_dest_opts:
                opt_val = getattr(args, opt)
                if isinstance(opt_val, list):
                    for sub_val in opt_val:
                        src_paths.append(path.abspath(sub_val))
                else:
                    src_paths.append(path.abspath(opt_val))
                         
            dest_dir = get_common_parent(src_paths)
        
        #Figure out the working directory
        wd_base_dir = args.wd_root
        if wd_base_dir is None:
            wd_base_dir = dest_dir
        self.working_dir = path.join(wd_base_dir, 
                                     get_working_dir_name(self.prog_name, 
                                                          base_in_hash,
                                                          args.wd_suffix)
                                    )
        
        self.keep_wd = args.keep_wd
        self.exec_plugin = args.exec_plugin
            
        return args, dest_dir
    
    def run(self, wf, wf_resources=None, node_resources=None):
        '''Run the given workflow. Can also specify a default run time 
        limit and memory limit to use for any nodes that don't have 
        specialized 'plugin_args'.'''
        #Set the working dir
        wf.base_dir = self.working_dir
        
        #Write out the graph
        wf.write_graph()
        
        #If the run might be distributed, increase the timeout to 
        #account for networked file system caching
        if not self.exec_plugin in non_dist_plugins:
            wf.config['execution'] = {'job_finished_timeout': 60.0}
        
        #Get the plugin args for the workflow 
        wf_args = get_full_plugin_args(self.exec_plugin, wf_resources)
        
        #Set plugin args for any specialized nodes
        for node_name, node_req in node_resources.iteritems():
            node = wf.get_node(node_name)
            if node_req.use_mpi:
                node.inputs.use_mpi = True
            node_args = get_full_plugin_args(self.exec_plugin, node_req)
            node.plugin_args = {'qsub_args' : node_args,                                 
                                'overwrite' : True
                               }

        #Run the workflow, keep the working directory if there is an 
        #error or it is explicitly requested
        try:
            wf.run(plugin=self.exec_plugin, 
                   plugin_args={'qsub_args' : wf_args}
                  )
        except Exception:
            print ("Exception occured, not automatically cleaning up "
                   "working dir %s" % self.working_dir)
            raise
        else:
            if not self.keep_wd:
                print ("Pipeline ran without error, cleaning up working "
                       "dir %s" % self.working_dir)
                shutil.rmtree(self.working_dir)
                
            return 0
            

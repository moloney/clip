'''Example for a local site configuration. Set the environment 
variable CLIP_CONF to the path for your site configuration file.'''

cli_defaults = {'wd_root' : '/scratch',
                'exec_plugin' : 'SGE',
               }

default_plugin_args = {'SGE' : '-b n',
                       'SGEGraph' : '-b n',
                      }
'''Default arguments for various plugins'''

def get_plugin_args(exec_plugin, req):
    '''Get the appropriate plugin_args for the given resource request.
    '''
    if exec_plugin in ('Debug', 'Linear', 'MultiProc'):
        return ''
    
    result = []
    if exec_plugin in ('SGE', 'SGEGraph'):
        #Create the -l option string
        l_res = []
        if not req.time is None:
            l_res.append('h_rt=%d' % req.time)
        if not req.mem is None:
            l_res.append('mf=%d' % req.mem)
        if not req.vmem is None:
            l_res.append('h_vmem=%d' % req.vmem)
        if l_res:
            result.append('-l %s' % ','.join(l_res))
        
        #Create the -pe option string
        if not req.min_cores == 1 or not req.max_cores is None:
            pe_res = ['-pe']
            if req.use_mpi:
                pe_res.append('mpi')
            else:
                pe_res.append('smp')
            if not req.max_cores is None:
                pe_res.append('%d-%d' % (req.min_cores, req.max_cores))
            else:
                pe_res.append(str(req.min_cores))
            result.append(' '.join(pe_res))
    else:
        raise ValueError("This execution plugin is not supported")
                
    return ' '.join(result)

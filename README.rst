.. -*- rest -*-
.. vim:syntax=rest

====
clip
====

Simpler Command Line Interfaces to Pipelines.

This module simplifies the creation of a Command Line Interface (CLI) 
to a Nipype pipeline. It handles common concerns like handling the 
pipeline working directory and allowing different execution plugins 
to be used.

Local site configuration
------------------------

By defining a environment variable "CLIP_CONF" that gives the path to 
a python file, local site specific configuration can be handled. You 
can take a look at the included "clip_conf_example.py" for an example 
of what this might look like.


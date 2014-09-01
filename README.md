[![Build Status](https://travis-ci.org/rymurr/q.svg?branch=master)](https://travis-ci.org/rymurr/q)
[![Coverage Status](https://coveralls.io/repos/rymurr/q/badge.png)](https://coveralls.io/r/rymurr/q)

## ABOUT ##

The q package is a q connection library implemented in Python.

## Current Status
The api to q is done though currently the compression that kdb uses is not working. So it really only works locally. Currently working on:

 * building compression algo
 * bug fixing and testing
 * started a python-esque to q transpiler
 * integrate traspiler into a pandas data frame, ipython engine, ipython magics, own stand alone language that sits as an extra layer between q/kdb and humans
## LICENSE ##

This code is licensed under an MIT license.  See LICENSE for
the full text.

## MISC ##

This project was originally a fork of Dan Nugets q library (http://github.com/nugend/q) which was a rewrite of Matt Warren's qPy (http://bitbucket.org/halotis/qpy) library. The current incarnation of this code has nothing in common with either project but the original authors still deserve credit. The main changes are to make the q connection wrapper more compliant with DB-API 2.0 and to try and hide some of the scary k/q stuff. There is also a much more pythonic and performant serializer/deserializer.

## CONTACT ##

Ryan Murray
rymurr@gmail.com

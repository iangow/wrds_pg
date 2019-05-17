#!/usr/bin/env python3
from sqlalchemy import create_engine
import os, sys
from wrds2pg import wrds2pg

wrds2pg.wrds_update("globalvoteresults", "risk")
wrds2pg.wrds_update("vavoteresults", "risk")
wrds2pg.wrds_update("issrec", "risk")

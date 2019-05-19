#!/usr/bin/env bash
printf "Updating Audit Analytics (audit) ...\n"
cd audit
./update_audit.py
printf "\nUpdating CRSP (crsp) ...\n"
cd ../crsp
./update_crsp.py
printf "\nUpdating DealScan (dealscan) ...\n"
cd ../dealscan
./update_dealscan.py
printf "\nUpdating IBES (ibes) ...\n"
cd ../ibes
./update_ibes.py
printf "\nUpdating RavenPack (rpna) ...\n"
cd ../rpna
./update_rpna.py
printf "\nUpdating Thomson Reuters (tfn) ...\n"
cd ../tfn
./update_tfn.py
cd ..

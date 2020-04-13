#!/usr/bin/env bash
printf "Updating Audit Analytics (audit) ...\n"
audit/update_audit.py
printf "\nUpdating CRSP (crsp) ...\n"
crsp/update_crsp.py
printf "\nUpdating DealScan (dealscan) ...\n"
dealscan/update_dealscan.py
printf "\nUpdating IBES (ibes) ...\n"
ibes/update_ibes.py
printf "\nUpdating RavenPack (rpna) ...\n"
rpna/update_rpna.py
printf "\nUpdating Thomson Reuters (tfn) ...\n"
tfn/update_tfn.py
printf "\nUpdating WRDS SEC Analytics (wrdssec) ...\n"
wrdssec/update_wrdssec.py
printf "\nUpdating MFLINKS (mflinks) ...\n"
mflinks/update_mflinks.py
printf "\nUpdating Capital IQ (ciq) ...\n"
ciq/update_ciq.py
printf "\nUpdating KLD (kld) ...\n"
kld/update_kld.sh
printf "\nUpdating TFN (tfn) ...\n"
tfn/update_tfn.sh

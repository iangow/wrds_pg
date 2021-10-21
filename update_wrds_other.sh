#!/usr/bin/env bash
printf "\nUpdating KLD (kld) ...\n"
kld/update_kld.py
printf "\nUpdating WRDS SEC data (wrdssec) ...\n"
wrdssec/update_wrdssec.py
printf "\nUpdating RiskMetrics data (risk) ...\n"
risk/update_risk_other.py
printf "\nUpdating Capital IQ key development data (ciq) ...\n"
ciq/update_ciq_keydev.py
printf "\nUpdating Capital IQ transcripts data (ciq) ...\n"
ciq/update_ciq_transcripts.py
printf "\nUpdating Compustat data (compa) ...\n"
compa/update_compa.py

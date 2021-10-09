#!/usr/bin/env bash
printf "\nUpdating KLD (kld) ...\n"
kld/update_kld.py
printf "\nUpdating WRDS SEC data (wrdssec) ...\n"
wrdssec/update_wrdssec.py
printf "\nUpdating RiskMetrics data (risk) ...\n"
risk/update_risk.py

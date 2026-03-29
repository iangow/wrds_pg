#!/usr/bin/env bash
printf "\nUpdating KLD (kld) ...\n"
uv run python kld/update_kld.py
printf "\nUpdating WRDS SEC data (wrdssec) ...\n"
uv run python wrdssec/update_wrdssec.py
printf "\nUpdating RiskMetrics data (risk) ...\n"
uv run python risk/update_risk_other.py
printf "\nUpdating Capital IQ key development data (ciq) ...\n"
uv run python ciq/update_ciq_keydev.py
printf "\nUpdating Capital IQ transcripts data (ciq) ...\n"
uv run python ciq/update_ciq_transcripts.py
printf "\nUpdating Compustat data (compa) ...\n"
uv run python compa/update_compa.py

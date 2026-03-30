#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir"

if [[ -f .env ]]; then
  set -a
  . ./.env
  set +a
fi

printf "Updating Audit Analytics (audit) ...\n"
uv run python audit/update_acc_oversight.py
uv run python audit/update_audit_compliance.py
uv run python audit/update_corporate_legal.py
printf "\nUpdating Capital IQ (ciq) ...\n"
uv run python ciq/update_ciq.py
printf "\nUpdating CRSP (crsp) ...\n"
uv run python crsp/update_crsp.py
printf "\nUpdating DealScan (dealscan) ...\n"
uv run python dealscan/update_dealscan.py
printf "\nUpdating Fama-French (ff) ...\n"
uv run python ff/update_ff.py
printf "\nUpdating IBES (ibes) ...\n"
uv run python ibes/update_ibes.py
printf "\nUpdating ISS voting (risk) ...\n"
bash risk/update.sh
# printf "\nUpdating RavenPack (rpna) ...\n"
# uv run python rpna/update_rpna.py
printf "\nUpdating Thomson Reuters (tfn) ...\n"
# uv run python tfn/update_tfn.py
printf "\nUpdating Compustat segment data (compsegd) ...\n"
uv run python compsegd/update_compsegd.py
printf "\nUpdating WRDS apps (wrdsapps) ...\n"
uv run python wrdsapps/update_wrdsapps.py

#!/usr/bin/env bash
# Starts repair.py as a Kubernetes Job and prints the Job name.
#
# Usage: run.sh dev|prod <repair.py arguments>
# Example: run.sh dev audit full
#
# The ConfigMap name holds a hash of the script, so a restarted pod always
# runs the same script as the first attempt.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)

case "${1:-}" in
  dev)
    context=witco-dev namespace=stratus secret=stratus-superuser
    host=stratus-am71-kube0-service datacenter=am71-kube0
    ;;
  prod)
    context=witco-prd namespace=cirrus secret=cirrus-superuser
    host=cirrus-am16-kube0-service datacenter=am16-kube0
    ;;
  *)
    echo "usage: $0 dev|prod <repair.py arguments>" >&2
    exit 2
    ;;
esac
shift

hash=$(shasum -a 256 "$here/repair.py" | cut -c1-10)
configmap="defer-repair-script-$hash"
kubectl --context "$context" -n "$namespace" create configmap "$configmap" \
  --from-file=repair.py="$here/repair.py" --dry-run=client -o yaml |
  kubectl --context "$context" apply -f - >/dev/null

args=$(python3 -c 'import json, sys; print(json.dumps(sys.argv[1:]))' "$@")

sed -e "s|__NAMESPACE__|$namespace|" \
  -e "s|__HOST__|$host|" \
  -e "s|__DATACENTER__|$datacenter|" \
  -e "s|__SECRET__|$secret|g" \
  -e "s|__CONFIGMAP__|$configmap|" \
  -e "s|__ARGS__|$args|" \
  "$here/job.yaml" |
  kubectl --context "$context" create -f - -o name

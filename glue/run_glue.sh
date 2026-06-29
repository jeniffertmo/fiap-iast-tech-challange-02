#!/usr/bin/env bash

_set_aws_from_profile() {
    local profile="$1"
    AWS_ACCESS_KEY_ID="$(aws configure get aws_access_key_id --profile $profile)"
    AWS_SECRET_ACCESS_KEY="$(aws configure get aws_secret_access_key --profile $profile)"
    AWS_SESSION_TOKEN="$(aws configure get aws_session_token --profile $profile 2> /dev/null || true)"
    AWS_DEFAULT_REGION="$(aws configure get region --profile $profile)"
}  

_check_aws_from_envvars() {
    : "${AWS_ACCESS_KEY_ID:?Error: AWS_ACCESS_KEY_ID is not set. Provide -p <profile> or export credentials.}"
    : "${AWS_SECRET_ACCESS_KEY:?Error: AWS_SECRET_ACCESS_KEY is not set. Provide -p <profile> or export credentials.}"
    : "${AWS_DEFAULT_REGION:?Error: AWS_DEFAULT_REGION is not set. Provide -p <profile> or export credentials.}"
    AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}"
}

usage() {
    echo "Usage: $0 [-p|--profile <aws-profile>] <glue_script.py>"
    exit 1
}

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AWS_PROFILE=""
GLUE_SCRIPT=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -p|--profile)
            [[ $# -lt 2 ]] && usage
            AWS_PROFILE="$2"
            shift 2
            ;;
        -*)
            echo "Unknown option: $1" >&2
            usage
            ;;
        *)
            [[ -n "$GLUE_SCRIPT" ]] && usage
            GLUE_SCRIPT="$1"
            shift
            ;;
    esac
done

[[ -z "$GLUE_SCRIPT" ]] && usage

GLUE_SCRIPT="$SCRIPT_DIR/$GLUE_SCRIPT"
if [[ ! -f "$GLUE_SCRIPT" ]]; then
    echo "Error: script '$GLUE_SCRIPT' not found." >&2
    exit 1
fi

if [[ -n "$AWS_PROFILE" ]]; then
    _set_aws_from_profile "$AWS_PROFILE"
else
    _check_aws_from_envvars
fi

SCRIPT_BASENAME="$(basename "$GLUE_SCRIPT")"
WORKSPACE_LOCATION="$SCRIPT_DIR"

echo "WORKSPACE_LOCATION : $WORKSPACE_LOCATION"
echo "AWS_DEFAULT_REGION : $AWS_DEFAULT_REGION"
echo ""

docker run -it --rm \
    -v "$WORKSPACE_LOCATION":/home/hadoop/workspace/ \
    -e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
    -e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
    -e AWS_DEFAULT_REGION="$AWS_DEFAULT_REGION" \
    ${AWS_SESSION_TOKEN:+-e AWS_SESSION_TOKEN="$AWS_SESSION_TOKEN"} \
    --name glue5_spark_submit \
    public.ecr.aws/glue/aws-glue-libs:5 \
    spark-submit /home/hadoop/workspace/"$SCRIPT_BASENAME"

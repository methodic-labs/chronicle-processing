#! /bin/bash
prefect cloud login -k $PREFECT_TK -w $PREFECT_WS
prefect agent start --work-queue default

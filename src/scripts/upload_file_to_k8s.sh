#!/bin/bash
# [MANUAL] change mode to ReadWriteOnce -> kubectl edit pv pvc-55cb3647-3dce-11ea-b421-42010a84004a -n hsbc-k8s-deployment
kubectl apply -f ./pod-to-upload.yml -n hsbc-k8s-deployment
sleep 20
kubectl cp $1 hsbc-k8s-deployment/spark-data-pod:$2
kubectl exec spark-data-pod -- ls -lar /data
kubectl delete pod spark-data-pod -n hsbc-k8s-deployment
# [MANUAL] change mode to ReadOnlyMany -> kubectl edit pv pvc-55cb3647-3dce-11ea-b421-42010a84004a -n hsbc-k8s-deployment
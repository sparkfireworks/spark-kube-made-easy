#!/bin/bash
kubectl apply -f ./pod.yml -n hsbc-k8s-deployment
sleep 20
kubectl exec spark-data-pod -n hsbc-k8s-deployment ls  $1
kubectl delete pod spark-data-pod -n hsbc-k8s-deployment

spark-submit --master k8s://https://kubernetes-api:6443 --deploy-mode cluster \
  --conf spark.kubernetes.container.image=your-spark-image \
  --packages io.github.neutrinic:apilytics_2.13:1.0.0 \
  --conf spark.jars.ivy=/tmp/ivy \
  --conf spark.kubernetes.driver.node.selector.kubernetes.io/hostname=node-with-config \
  --conf spark.kubernetes.driver.volumes.hostPath.apilytics.mount.path=/etc/apilytics \
  --conf spark.kubernetes.driver.volumes.hostPath.apilytics.mount.readOnly=true \
  --conf spark.kubernetes.driver.volumes.hostPath.apilytics.options.path=/srv/apilytics \
  --conf spark.sql.catalog.api=com.apilytics.spark.RESTCatalog \
  --conf spark.sql.catalog.api.config=/etc/apilytics/api.conf \
  local:///opt/jobs/your-job.py

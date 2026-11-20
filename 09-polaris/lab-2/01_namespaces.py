# Both catalogs reachable: "lab" lists directories of the bucket, "polaris" asks the REST API
spark.sql("SHOW NAMESPACES IN lab").show()
spark.sql("SHOW NAMESPACES IN polaris").show()

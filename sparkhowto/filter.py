#Dummy
df.filter(df("state") === "OH").show(false)
df.filter('state === "OH").show(false)
df.filter($state === "OH").show(false)
df.filter(col("state") === "OH").show(false)
df.where(df("state") === "OH").show(false)
df.where('state === "OH").show(false)
df.where($state === "OH").show(false)
df.where(col("state") === "OH").show(false)

df.filter("gender == 'M'").show(false)
df.where("gender == 'M'").show(false)

//multiple condition
df.filter(df("state") === "OH" && df("gender") === "M")
    .show(false)
    
    import org.apache.spark.sql.functions.array_contains
df.filter(array_contains(df("languages"),"Java"))
    .show(false)

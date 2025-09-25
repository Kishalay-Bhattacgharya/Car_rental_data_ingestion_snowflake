from pyspark.sql import SparkSession
from pyspark.sql.functions import col,datediff,when,lit
import argparse



def load_to_rentals_fact(execution_date):
   spark=SparkSession.builder.appName('Load_To_Snowflake').getOrCreate()


   #To only process the files as per the date which gets passed from the airflow dag as a parameter...
   gcs_path='gs://snowflake_projects_kishalay/car_rentals_data/rentals_daily/car_rental_{}.json'.format(execution_date)


   raw_df=spark.read.option('multiline','true').json(gcs_path)

   raw_df.head(2)
   validated_df=raw_df.filter(
      col('rental_id').isNotNull() &
      col('customer_id').isNotNull() &
      col('car.make').isNotNull() &
      col('car.model').isNotNull() &
      col('car.year').isNotNull() &
      col('rental_period.start_date').isNotNull() &
      col('rental_period.end_date').isNotNull() &
      col('rental_location.pickup_location').isNotNull() &
      col('rental_location.dropoff_location').isNotNull() &
      col('amount').isNotNull() &
      col('quantity').isNotNull()

   )

   transformed_df=validated_df.withColumn(
      'rental_duration_days',datediff(col('rental_period.end_date'),col('rental_period.start_date'))
   ).withColumn(
      'total_rental_amount',col('amount')* col('quantity')
   ).withColumn(
      'average_rental_amount',col('total_rental_amount')/col('rental_duration_days')
   ).withColumn(
        "is_long_rental", 
        when(col("rental_duration_days") > 7, lit(1)).otherwise(lit(0))
    )

   connection_details={
        "sfURL": "https://---------.snowflakecomputing.com",
        "sfAccount": "---------",
        "sfUser": "----------",
        "sfPassword": "----------",
        "sfDatabase": "car_rental",
        "sfSchema": "PUBLIC",
        "sfWarehouse": "COMPUTE_WH",
        "sfRole": "ACCOUNTADMIN"
   }

   car_df=spark.read.format('snowflake')\
               .options(**connection_details)\
               .option('dbtable','dim_car').load()
   

   location_df=spark.read.format('snowflake')\
               .options(**connection_details)\
               .option('dbtable','dim_location').load()
   
   date_df=spark.read.format('snowflake')\
               .options(**connection_details)\
               .option('dbtable','dim_date').load()
   
   customer_df=spark.read.format('snowflake')\
               .options(**connection_details)\
               .option('dbtable','dim_customer').load()
   
   fact_rentals_df=transformed_df.alias("rentals").\
                        join(car_df.alias("cars"),
                                      (col('cars.make')==col('rentals.car.make')) &
                                      (col('cars.model')==col('rentals.car.model')) &
                                      (col('cars.year')==col('rentals.car.year'))
                                      ,how='inner')\
                                      .drop(col('cars.make'),col('rentals.car.make'),col('cars.model')
                                            ,col('rentals.car.model'),col('cars.year'),col('rentals.car.year'))                
   

   #To join all the dimension and fact tables and only select the foreign key in the fact table..
   fact_rentals_df=fact_rentals_df.alias('rentals')\
                                .join(location_df.alias('location'),
                                      (col('rentals.rental_location.pickup_location')==col('location.location_name')),how='inner')\
                                .withColumnRenamed('location_key','pickup_location_key')


   fact_rentals_df=fact_rentals_df.alias('rentals')\
                                .join(location_df.alias('loc'),
                                      (col('rentals.rental_location.dropoff_location')==col('loc.location_name'))
                                      )    \
                                .withColumnRenamed('location_key','drop_location_key')                                   
   fact_rentals_df=fact_rentals_df.alias('rentals')\
                      .join(customer_df.alias('cust')
                            ,(col('cust.customer_id')==col('rentals.customer_id'))
                            )
   fact_rentals_df = fact_rentals_df.alias("fact") \
        .join(date_df.alias("start_date_dim"), col("fact.start_date") == col("start_date_dim.date"), "left") \
        .withColumnRenamed("date_key", "start_date_key") \
        .drop("start_date")

    # Join with date_dim for end_date
   fact_rentals_df = fact_rentals_df.alias("fact") \
        .join(date_df.alias("end_date_dim"), col("fact.end_date") == col("end_date_dim.date"), "left") \
        .withColumnRenamed("date_key", "end_date_key") \
        .drop("end_date")    

   #To only select necessary columns
   fact_rentals_df=fact_rentals_df.select(
       "rental_id",
        "customer_key",
        "car_key",
        "pickup_location_key",
        "drop_location_key",
        "start_date_key",
        "end_date_key",
        "amount",
        "quantity",
        "rental_duration_days",
        "total_rental_amount",
        "average_rental_amount",
        "is_long_rental"
   )   

   #To write this back to snowflake fact_rentals table..
   fact_rentals_df.write.format('snowflake')\
                  .options(**connection_details)\
                  .option('dbtable','fact_rentals').mode('append').save()
   
if __name__=='__main__':
      parser=argparse.ArgumentParser(description='Execution date')
      parser.add_argument('--date',type=str,required=True)

      args=parser.parse_args()
      load_to_rentals_fact(args.date)


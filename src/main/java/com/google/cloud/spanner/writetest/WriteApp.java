package com.google.cloud.spanner.writetest;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Value;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;

public class WriteApp {

  // Run with this command:
  //
  // mvn exec:java

  // CREATE TABLE all_types (
  //  col_bigint bigint NOT NULL,
  //  col_bool boolean,
  //  col_bytea bytea,
  //  col_float8 double precision,
  //  col_int bigint,
  //  col_numeric numeric,
  //  col_timestamptz timestamp with time zone,
  //  col_date date,
  //  col_varchar character varying(100),
  //  col_jsonb jsonb,
  //  PRIMARY KEY(col_bigint)
  //);
  //
  // CREATE CHANGE STREAM all_types_change_stream
  // FOR all_types;

  public static void main(String[] args) throws Exception {
    SpannerOptions options = SpannerOptions.newBuilder()
        .setProjectId("appdev-soda-spanner-staging")
        .build();
    try (Spanner spanner = options.getService()) {
      DatabaseClient client = spanner.getDatabaseClient(DatabaseId.of("appdev-soda-spanner-staging", "knut-test-ycsb", "knut-test-db"));
      for (int i = 0; i < 100; i++) {
        Thread.sleep(ThreadLocalRandom.current().nextInt(1, 5));
        Stopwatch stopwatch = Stopwatch.createStarted();
        client.writeAtLeastOnce(ImmutableList.of(Mutation.newInsertOrUpdateBuilder("all_types")
            .set("col_bigint").to(ThreadLocalRandom.current().nextLong())
            .set("col_int").to(ThreadLocalRandom.current().nextInt())
            .set("col_float8").to(ThreadLocalRandom.current().nextDouble())
            .set("col_bytea").to(ByteArray.copyFrom(UUID.randomUUID().toString()))
            .set("col_numeric").to(Value.pgNumeric(String.valueOf(ThreadLocalRandom.current().nextInt())))
            .set("col_date").to(Date.fromJavaUtilDate(new java.util.Date(ThreadLocalRandom.current().nextInt(1_000_000))))
            .set("col_timestamptz").to(Timestamp.of(new java.util.Date(ThreadLocalRandom.current().nextInt(1_000_000))))
            .set("col_jsonb").to(String.format("{\"value\":\"%s\"}", RandomStringUtils.insecure().next(5000, true, true)))
            .set("col_bool").to(Boolean.FALSE)
            .set("col_varchar").to(UUID.randomUUID().toString())
            .build()));
        System.out.println("Elapsed: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms");
      }
    }
  }

}

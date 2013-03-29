# -*- coding: utf-8 -*-
module Fluent


class DynamoDBPPCOutput < Fluent::BufferedOutput
  Fluent::Plugin.register_output('dynamodbppc', self)

  include DetachMultiProcessMixin

  BATCHWRITE_ITEM_LIMIT = 25
  BATCHWRITE_CONTENT_SIZE_LIMIT = 1024*1024

  def initialize
    super
    require 'aws-sdk'
    require 'msgpack'
    require 'time'
    require 'uuidtools'
  end

  config_param :aws_key_id, :string
  config_param :aws_sec_key, :string
  config_param :proxy_uri, :string, :default => nil
  config_param :dynamo_db_table, :string
  config_param :dynamo_db_endpoint, :string, :default => nil
  config_param :time_format, :string, :default => nil
  config_param :detach_process, :integer, :default => 2

  def configure(conf)
    super

    @timef = TimeFormatter.new(@time_format, @localtime)
  end

  def start
    options = {
      :access_key_id      => @aws_key_id,
      :secret_access_key  => @aws_sec_key,
      :dynamo_db_endpoint => @dynamo_db_endpoint,
    }
    options[:proxy_uri] = @proxy_uri if @proxy_uri

    detach_multi_process do
      super

      begin
        restart_session(options)
        valid_table(@dynamo_db_table)
      rescue ConfigError => e
        $log.fatal "ConfigError: Please check your configuration, then restart fluentd. '#{e}'"
        exit!
      rescue Exception => e
        $log.fatal "UnknownError: '#{e}'"
        exit!
      end
    end
  end

  def restart_session(options)
    config = AWS.config(options)
    @batch = AWS::DynamoDB::BatchWrite.new(config)
    @dynamo_db = AWS::DynamoDB.new(options)
  end

  def valid_table(table_name)
    table = @dynamo_db.tables[table_name]
    table.load_schema
    raise ConfigError, "Binary hash keys not supported" if table.range_key.type.to_s == 'binary'
    raise ConfigError, "Database range key must be an Integer, it will hold a timestamp" if table.range_key.type.to_s != 'number'
    @hash_key_value = table.hash_key.name
    @hash_range_value = table.range_key.name
  end

  def format(tag, time, record)
    record[@hash_range_value] = time
    record.to_msgpack
  end

  def write(chunk)
    batch_size = 0
    batch_records = []
    chunk.msgpack_each {|record|
      #Skip bad records
      next unless (record[@hash_key_value]!= "" && record[@hash_key_value]!= nil)

      batch_records << record
      batch_size += record.to_json.length # FIXME: heuristic
      if batch_records.size >= BATCHWRITE_ITEM_LIMIT || batch_size >= BATCHWRITE_CONTENT_SIZE_LIMIT
        batch_put_records(batch_records)
        batch_records.clear
        batch_size = 0
      end
    }
    unless batch_records.empty?
      batch_put_records(batch_records)
    end
  end

  def batch_put_records(records)
    @batch.put(@dynamo_db_table, records)
    @batch.process!
  end

end


end

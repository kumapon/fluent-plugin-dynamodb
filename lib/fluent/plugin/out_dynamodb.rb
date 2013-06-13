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

    config_param :aws_key_id, :string, :default => nil
    config_param :aws_sec_key, :string, :default => nil
    config_param :proxy_uri, :string, :default => nil
    config_param :dynamo_db_table, :string
    config_param :dynamo_db_endpoint, :string, :default => nil
    config_param :time_format, :string, :default => nil
    config_param :detach_process, :integer, :default => 2
    config_param :dynamo_db_hash_key, :string, :default => nil
    config_param :dynamo_db_range_key, :string, :default => nil
    config_param :dynamo_array_flattener, :string, :default => nil

    def configure(conf)
      super

      @timef = TimeFormatter.new(@time_format, @localtime)
    end

    def start
      options = {
        :dynamo_db_endpoint => @dynamo_db_endpoint,
      }

      if @aws_key_id && @aws_sec_key
        options[:access_key_id] =  @aws_key_id
        options[:secret_access_key] = @aws_sec_key
      end

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

    #Since we want to have a different behaviour for two tables from the same request write method gets a little bit messy
    def write(chunk)
      batch_size = 0
      batch_records = []
      chunk.msgpack_each {|record|
        next if record["table"] == "conversion" #Skipping conversion records, those should be handled by another request
        if @dynamo_db_table.include?("number") #dirty dirty dirty, shame on me!
          unique_numbers = CGI.unescape(record['phone_numbers']).split(';').uniq rescue []
          unique_numbers.each do |number|
            #Skip bad records
            next unless (number != "" && number != nil)
            next unless (record[@hash_range_value]!= "" && record[@hash_range_value]!= nil)
            batch_records << [:number => number.gsub('-', '').gsub(' ', ''),
                              :timestamp => record['timestamp'],
                              :session_id => record['session_id']]
          end
        else
          #Skip bad records
          next unless (record[@hash_key_value]!= "" && record[@hash_key_value]!= nil)
          next unless (record[@hash_range_value]!= "" && record[@hash_range_value]!= nil)
          batch_records << build_session_object(record) rescue puts "Somewhing went wrong creating the session object"
        end

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
      @batch.put(@dynamo_db_table, records.uniq)
      @batch.process!
    end

    def default_null(hash, key)
      (hash[key] == nil || hash[key] == '')  ? "null" : hash[key]
    end

  def build_session_object(record)
      parsed_time = record["parsed_time"]
      parsed_time ||= Time.now.to_s #Just in case
      date_time = DateTime.parse(parsed_time)
      date_time ||= DateTime.now #Just in case
      {
       'page_id' => default_null(record, 'landing_page_id').to_s,
       'timestamp' => date_time.to_time.to_i,
       'visit_id' => SecureRandom.uuid,
       'session_id' => default_null(record, 'session_id'),
       'merchant_id' => default_null(record, 'merchant_id'),
       'program_id' => default_null(record, 'program_id'),
       'medium_id' => default_null(record, 'medium_id'),
       'om_external' => default_null(record, 'om_external'),
       'campaign_id' => default_null(record, 'campaign_id'),
       'phone_numbers' => default_null(record, 'phone_numbers'),
       'url' => default_null(record, 'url'),
       'visitor_type' => default_null(record, 'visitor_type'),
       'referrer' => CGI.unescape(default_null(record, 'referrer')),
       'browser' => default_null(record, 'browser'),
       'os' => default_null(record, 'os'),
       'is_mobile' => default_null(record, 'is_mobile'),
       'language' => default_null(record, 'language'),
       'date' => date_time.to_date.to_s,
       'user_agent' => default_null(record, 'user_agent'),
       'ip_address' => default_null(record, 'remote'),
       'visit_count' => default_null(record, 'session_count'),
       'days_since_last_visit' => (((date.to_date - (Time.at(record['last_visit'].to_i / 1000).to_date)).to_i) rescue "0")
      }
    end

  end #Class
end #Module

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
    config_param :dynamo_db_endpoint, :string, :default => nil
    config_param :time_format, :string, :default => nil
    config_param :detach_process, :integer, :default => 2
    config_param :dynamo_db_numbers, :string
    config_param :dynamo_db_sessions, :string
    config_param :dynamo_db_numbers_hash_key, :string, :default => nil
    config_param :dynamo_db_numbers_range_key, :string, :default => nil
    config_param :dynamo_db_sessions_hash_key, :string, :default => nil
    config_param :dynamo_db_sessions_range_key, :string, :default => nil
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
          valid_table(@dynamo_db_numbers)
          valid_table(@dynamo_db_sessions)
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
      @batch_numbers = AWS::DynamoDB::BatchWrite.new(config)
      @batch_sessions = AWS::DynamoDB::BatchWrite.new(config)
      @dynamo_db = AWS::DynamoDB.new(options)
    end

    def valid_table(table_name)
      table = @dynamo_db.tables[table_name]
      table.load_schema
      raise ConfigError, "Binary hash keys not supported" if table.range_key.type.to_s == 'binary'
      raise ConfigError, "Database range key must be an Integer, it will hold a timestamp" if table.range_key.type.to_s != 'number'
    end

    def format(tag, time, record)
      record[@hash_range_value] = time
      record.to_msgpack
    end

    def write(chunk)
      batch_number_records = []
      batch_session_records = []
      chunk.msgpack_each{|record|
        next if record["table"] == "conversion"
        begin
          parsed_time = record["parsed_time"]
          parsed_time ||= Time.now.to_s #Just in case

          unique_numbers = CGI.unescape(record['phone_numbers']).split(';').uniq rescue []
          unique_numbers.each do |number|
            #Skip bad records
            next unless (number != "" && number != nil)
            #Create Number Record
            batch_number_records << {:number => number.gsub('-', '').gsub(' ', ''),
                                     :timestamp => Time.parse(parsed_time).to_i,
                                     :session_id => record['session_id']}
            #Create Session Record
            batch_session_records << build_session_object(record, parsed_time)

            #Check if we have enough to push
            if batch_number_records.size >= BATCHWRITE_ITEM_LIMIT or batch_session_records.size >= BATCHWRITE_ITEM_LIMIT
              batch_put_records(batch_number_records, batch_session_records)
              batch_session_records.clear
              batch_number_records.clear
            end
          end
        rescue Exception => e
          $log.fatal("Somewhing went wrong, error: #{e.message}")
          raise e
        end
      }
      batch_put_records(batch_number_records, batch_session_records) if (batch_session_records.any? or batch_number_records.any?)
      batch_session_records.clear
      batch_number_records.clear
    end

    def batch_put_records(numbers, sessions)
      @batch_numbers.put(@dynamo_db_numbers, numbers.uniq{|e| [e[:number], e[:timestamp]]})
      @batch_numbers.process!
      @batch_sessions.put(@dynamo_db_sessions, sessions.uniq{|e| [e['session_id'], e['timestamp']]})
      @batch_sessions.process!
    end

    def default_null(hash, key)
      (hash[key] == nil || hash[key] == '')  ? "null" : hash[key]
    end

    def build_session_object(record, parsed_time)
      date_time = DateTime.parse(parsed_time)

      {
       'page_id' => default_null(record, 'landing_page_id').to_s,
       'timestamp' => Time.parse(parsed_time).to_i,
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

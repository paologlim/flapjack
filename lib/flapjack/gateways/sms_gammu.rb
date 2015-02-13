#!/usr/bin/env ruby

require 'mysql2'

require 'flapjack/redis_proxy'
require 'flapjack/record_queue'
require 'flapjack/utility'
require 'flapjack/exceptions'

require 'flapjack/data/alert'
require 'flapjack/data/check'

module Flapjack
  module Gateways
    class SmsGammu
      INSERT_QUERY = <<-SQL
        INSERT INTO outbox (InsertIntoDB, TextDecoded, DestinationNumber, CreatorID, Class)
        VALUES ('%s', '%s', '%s', '%s', %s)
      SQL

      attr_accessor :sent

      include Flapjack::Utility

      def initialize(opts = {})
        @lock   = opts[:lock]
        @config = opts[:config]
        @queue  = Flapjack::RecordQueue.new(@config['queue'] || 'sms_gammu_notifications',
                                            Flapjack::Data::Alert)
        @sent   = 0
        @db     = Mysql2::Client.new(:host     => @config["mysql_host"],
                                     :database => @config["mysql_database"],
                                     :username => @config["mysql_username"],
                                     :password => @config["mysql_password"])

        Flapjack.logger.debug("new sms_gammu gateway pikelet with the following options: #{@config.inspect}")
      end

      def start
        begin
          Zermelo.redis = Flapjack.redis

          loop do
            @lock.synchronize do
              @queue.foreach {|alert| handle_alert(alert) }
            end

            @queue.wait
          end
        ensure
          Flapjack.redis.quit
        end
      end

      def stop_type
        :exception
      end

      private

      def handle_alert(alert)
        @alert  = alert
        address = alert.medium.address
        from    = @config["from"]
        message = prepare_message
        errors  = []

        [[from,    "SMS from address is missing"],
         [address, "SMS address is missing"]].each do |val_err|

          next unless val_err.first.nil? || (val_err.first.respond_to?(:empty?) && val_err.first.empty?)
          errors << val_err.last
        end

        unless errors.empty?
          errors.each {|err| Flapjack.logger.error err }
          return
        end

        send_message(message, from, address)
      rescue => e
        Flapjack.logger.error "Error generating or delivering sms to #{address}: #{e.class}: #{e.message}"
        Flapjack.logger.error e.backtrace.join("\n")
        raise
      end

      def prepare_message
        message_type  = @alert.rollup ? 'rollup' : 'alert'
        template_path = @config['templates']["#{message_type}.text"]
        template      = ERB.new(File.read(template_path), nil, '-')

        begin
          message = template.result(binding).chomp
          truncate(message, 159)
        rescue => e
          Flapjack.logger.error "Error while excuting the ERB for an sms: " +
            "ERB being executed: #{template_path}"
          raise
        end
      end

      def send_message(message, from, to)
        begin
          @db.query(INSERT_QUERY % [Time.now, message, to, from, 1])
          @sent += 1
          Flapjack.logger.debug "Sent SMS via Gammu"
        rescue => e
          Flapjack.logger.error "Failed to send SMS via Gammu: #{e.class}, #{e.message}"
        end
      end

    end
  end
end

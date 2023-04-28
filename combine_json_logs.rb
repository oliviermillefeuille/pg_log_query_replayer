# frozen_string_literal: true
require "optparse"
require "time"

class CombineJsonLogs
  def initialize
    @log_file_handlers = []
    @timestamps = []
    @logs = []
  end

  def main(args)
    open_all_log_files(args)
    read_first_line_from_each_file_and_init_timestamps
    combine_logs
  end

  private

  def open_all_log_files(files)
    files.each { |file| @log_file_handlers << File.open(file) }
  end

  def read_first_line_from_each_file_and_init_timestamps
    @log_file_handlers.each do |file|
      log = file.gets
      @logs << log
      timestamp = Time.parse(/(.*UTC)/.match(log)[1])
      @timestamps << timestamp
    end
  end

  def combine_logs
    until @log_file_handlers.empty?
      farthest = get_farthest_timestamp

      # loop in the reverse order to avoid index change when deleting elements
      @timestamps.reverse_each.with_index do |timestamp, index|
        if timestamp == farthest
          fetch_until_next_timestamp_from_file(@timestamps.length - 1 - index)
        end
      end
    end
  end

  def get_farthest_timestamp
    @timestamps.min
  end

  def fetch_until_next_timestamp_from_file(index)
    puts @logs[index]
    loop do
      log = @log_file_handlers[index].gets

      # remove all data for this file if it is EOF
      if log.nil?
        @logs.delete_at(index)
        @log_file_handlers.delete_at(index)
        @timestamps.delete_at(index)
        return
      end

      match = /(.*UTC)/.match(log[0..18])

      # update timestamp and log once a new timestamp is found
      if match
        @logs[index] = log
        @timestamps[index] = Time.parse(match[1])
        break

        # continue to print log if no timestamp is found (e.g. log is incomplete due to a multi-line query)
      else
        puts log
      end
    end
  end
end

if __FILE__ == $0
  combiner = CombineJsonLogs.new
  combiner.main(ARGV)
end

# frozen_string_literal: true
require 'json'
require 'optparse'
require 'ostruct'
require 'pg'
require 'pg_query'

class JsonQueryReplayer
  EXPLAIN_PLAN_FIELDS_TO_EXTRACT = {
    "Actual Total Time" => :total_time,
    "Total Cost" => :total_cost,
    "Shared Hit Blocks" => :total_shared_hit_blocks,
    "Shared Read Blocks" => :total_shared_read_blocks
  }.freeze

  def initialize(options)
    @options = options
    @connection = nil
    @all_stats = {}
    @execution_number = 0
    @start_time = Time.now
  end

  def main
    replay_queries
    display_top_100_queries_by_total_cost
    display_top_100_queries_by_count
  end

  private

  def replay_queries
    puts "# REPLAYING JSON FILE #{(@options.skip_lines ? "(skipping #{@options.skip_lines} lines)" : '')}"
    puts 'elapsed_in_secs,execution_number,line_number,fingerprint,count,cost,avg_cost,time,avg_time,shared_hit_blocks,avg_shared_hit_blocks,shared_read_blocks,avg_shared_read_blocks'

    parse_select_statements_from_pg_log(@options.log_file) do |line_number, query|
      handle_query(line_number, query)
    end
  end

  def handle_query(line_number, query)
    begin
      exec_info = execute_query_with_plan(query)
      fingerprint = PgQuery.fingerprint(query)

      query_stats = update_stats(query, fingerprint, exec_info)
      csv_values = build_csv_values(query_stats, exec_info)

      puts "#{Time.now - @start_time},#{@execution_number += 1},#{line_number},#{fingerprint},#{@all_stats[fingerprint][:count]}," + csv_values.join(',')
      true

    rescue StandardError => e
      raise unless e.message !=~ /ERROR:  missing FROM-clause entry/
      false
    end
  end

  def update_stats(query, fingerprint, exec_info)
    query_stats = @all_stats[fingerprint] ||= {statement: query, count:0, total_cost: 0, total_time: 0,
                                              total_shared_hit_blocks: 0, total_shared_read_blocks: 0}
    query_stats[:count] += 1
    EXPLAIN_PLAN_FIELDS_TO_EXTRACT.each do |_label, symbol|
      query_stats[symbol] += exec_info[symbol]
    end
    query_stats
  end

  def build_csv_values(query_stats, exec_info)
    csv_values = []
    EXPLAIN_PLAN_FIELDS_TO_EXTRACT.each do |_label, symbol|
      csv_values << exec_info[symbol]
      csv_values << query_stats[symbol] / query_stats[:count]
    end
    csv_values
  end

  def display_top_100_queries_by_total_cost
    puts ''
    puts '# TOTAL_COST : show the top 100 queries'
    puts 'fingerprint,statement,count,avg_cost,avg_time,avg_shared_hit_blocks,avg_shared_read_blocks'
    @all_stats.sort_by { |k, v| v[:total_cost] }.reverse.first(100).each do |fingerprint, query_stats|
      puts "#{fingerprint},#{query_stats[:statement][0..150]},#{query_stats[:count]},#{query_stats[:total_cost]},#{query_stats[:total_time]},#{query_stats[:total_shared_hit_blocks]},#{query_stats[:total_shared_read_blocks]}"
    end
  end

  def display_top_100_queries_by_count
    puts ''
    puts '# COUNT : show the top 100 queries'
    puts 'fingerprint,statement,count,avg_cost,avg_time,avg_shared_hit_blocks,avg_shared_read_blocks'
    @all_stats.sort_by { |k, v| v[:count] }.reverse.first(100).each do |fingerprint, query_stats|
      puts "#{fingerprint},#{query_stats[:statement][0..150]},#{query_stats[:count]},#{query_stats[:total_cost]},#{query_stats[:total_time]},#{query_stats[:total_shared_hit_blocks]},#{query_stats[:total_shared_read_blocks]}"
    end
  end

  def parse_select_statements_from_pg_log(log_file)
    line_number = 0
    json_in_progress = nil

    File.foreach(log_file) do |log|
      line_number += 1
      next unless should_process_line?(line_number)

      json_in_progress = handle_json_log(log, json_in_progress) do |json_object|
        query = json_object.dig('Query Text')
        next unless select_query?(query)
        yield(line_number, query) if block_given?
      end
    end
  end

  def handle_json_log(log, json_in_progress)
    if json_in_progress
      json_in_progress += log
    end

    if log.match?(/	}/)
      json_object = JSON.parse(json_in_progress)
      yield(json_object) if block_given?
      json_in_progress = nil

    elsif log.match?(/	{/)
      json_in_progress = log
    end

    json_in_progress
  end

  def should_process_line?(line_number)
    return false if @options.skip_lines && line_number <= @options.skip_lines
    return false if @options.max_lines && line_number > @options.max_lines
    true
  end

  def select_query?(query)
    query.match?(/^SELECT/)
  end

  def connection
    @connection ||= begin
      PG.connect(
        host: @options.pg_host,
        port: @options.pg_port,
        dbname: @options.pg_database,
        user: @options.pg_user,
        password: @options.pg_password,
        connect_timeout: 2
      )
    end
  end

  def execute_query_with_plan(query)
    json_plan = connection.exec("EXPLAIN (FORMAT JSON, ANALYZE, BUFFERS, VERBOSE) #{query}").first.first[1]
    json_plan_object = JSON.parse(json_plan).first["Plan"]

    exec_info = {}
    EXPLAIN_PLAN_FIELDS_TO_EXTRACT.each do |label, symbol|
      value = json_plan_object.dig(label).to_f
      exec_info[symbol.to_sym] = value
    end
    exec_info
  end

end

def get_options
  options = OpenStruct.new

  opt_parser = OptionParser.new do |opts|
    opts.banner = "Usage: log_query_replayer.rb --logfile LOGFILE [--skiplines NUMBER] --pghost HOST --pgport PORT --pgdatabase DATABASE --pguser USER"

    opts.separator ""
    opts.separator "Specific options:"
    opts.on("-l", "--logfile LOGFILE", "Require the LOGFILE") do |log_file|
      options.log_file = log_file
    end
    opts.on("-h", "--pghost PGHOST", "Require the PGHOST") do |pg_host|
      options.pg_host = pg_host
    end
    opts.on("-p", "--pgport PGPORT", "Require the PGPORT") do |pg_port|
      options.pg_port = pg_port
    end
    opts.on("-d", "--pgdatabase PGDATABASE", "Require the PGDATABASE") do |pg_database|
      options.pg_database = pg_database
    end
    opts.on("-u", "--pguser PGUSER", "Require the PGUSER") do |pg_user|
      options.pg_user = pg_user
    end
    opts.on("-w", "--pgpassword PGPASSWORD", "Require the PGPASSWORD") do |pg_password|
      options.pg_password = pg_password
    end
    opts.on("-s", "--skiplines NUMBER") do |skip_lines|
      options.skip_lines = skip_lines.to_i
    end
    opts.on("-m", "--maxlines NUMBER") do |max_lines|
      options.max_lines = max_lines.to_i
    end

    opts.separator ""
    opts.separator "Common options:"
    opts.on_tail("-h", "--help", "Show this message") do
      puts opts
      exit
    end
  end

  opt_parser.parse!(ARGV)

  mandatory_options = [:log_file, :pg_host, :pg_port, :pg_database, :pg_user]
  mandatory_options << :pg_password if options.pg_password
  unset_options = mandatory_options.select { |option| options[option].nil? }

  unless unset_options.empty?
    puts 'The following options are not set:'
    unset_options.each { |option| puts "  - #{option}" }
    puts 'Please provide all mandatory options. See --helputs for usage information.'
    exit
  end

  options
end

log_query_replayer = JsonQueryReplayer.new(get_options)
log_query_replayer.main

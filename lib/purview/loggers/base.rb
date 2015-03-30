module Purview
  module Loggers
    class Base
      def initialize(opts={})
        @opts = default_opts.merge(opts)
      end

      def debug(*args)
        log(DEBUG_LEVEL, *args) if debug?
      end

      def error(*args)
        log(ERROR_LEVEL, *args) if error?
      end

      def info(*args)
        log(INFO_LEVEL, *args) if info?
      end

      def with_context_logging(*args)
        debug(build_starting_message(*args))
        yield.tap { |result| debug(build_finished_message(*args)) }
      end

      private

      DEBUG_LEVEL = 'DEBUG'
      ERROR_LEVEL = 'ERROR'
      INFO_LEVEL = 'INFO'

      attr_reader :opts

      def build_finished_message(*args)
        case args.length
          when 1; "Finished #{args[0]}"
          when 2; args[-1]
          else; raise
        end
      end

      def build_message(level, *args)
        message, exception = args[0..1]
        message_template(!!exception) % {
          :exception => format_exception(exception),
          :level => level,
          :message => message,
          :process_id => Process.pid,
          :timestamp => Time.now.strftime('%Y-%m-%d %H:%M:%S.%L %z'),
        }
      end

      def build_starting_message(*args)
        case args.length
          when 1; "Starting #{args[0]}"
          when 2; args[0]
          else; raise
        end
      end

      def debug?
        !!opts[:debug]
      end

      def default_opts
        {
          :debug => true,
          :error => true,
          :info => false,
        }
      end

      def error?
        !!opts[:error]
      end

      def format_exception(exception)
        exception && exception.backtrace.map { |line| "\tfrom #{line}" }.join("\n")
      end

      def info?
        !!opts[:info]
      end

      def log(level, *args)
        stream.puts build_message(level, *args)
      end

      def message_template(exception)
        "%{timestamp} %{level} (%{process_id}) %{message}".tap do |result|
          result << ":\n%{exception}" if exception
        end
      end

      def stream
        opts[:stream]
      end
    end
  end
end

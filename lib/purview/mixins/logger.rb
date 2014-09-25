module Purview
  module Mixins
    module Logger
      def logger
       @logger ||= logger_type.new(logger_opts)
      end

      def logger_opts
        opts[:logger] || {}
      end

      def logger_type
        opts[:logger_type] || Purview::Loggers::Console
      end

      def with_context_logging(*args)
        logger.with_context_logging(*args) { yield }
      end
    end
  end
end

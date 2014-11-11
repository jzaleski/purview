module Purview
  module Columns
    class Base
      attr_reader :name

      def initialize(name, opts={})
        @name = name.to_sym
        @opts = default_opts.merge(opts)
      end

      def allow_blank?
        [nil, true].include?(opts[:allow_blank])
      end

      def default
        opts[:default]
      end

      def limit
        opts[:limit]
      end

      def parse(value)
        blank_value = blank_value?(value)
        return nil if blank_value && allow_blank?
        raise %{Unexpected blank value for column: "#{name}"} if blank_value
        type.parse(value)
      end

      def primary_key?
        !!opts[:primary_key]
      end

      def type
        opts[:type] || Purview::Types::String
      end

      private

      attr_reader :opts

      def blank_value?(value)
        value.nil? || value.strip.empty?
      end

      def default_opts
        {}
      end
    end
  end
end

module Purview
  module Columns
    class Base
      attr_reader :name

      def initialize(name, opts={})
        @name = name.to_sym
        @opts = default_opts.merge(opts)
      end

      def default
        opts[:default]
      end

      def default?
        !!default
      end

      def limit
        opts[:limit]
      end

      def limit?
        !!limit
      end

      def nullable
        coalesced(opts[:nullable], true)
      end

      def nullable?
        !!nullable
      end

      def parse(value)
        blank = blank?(value)
        return nil if blank && nullable?
        raise %{Unexpected blank value for column: "#{name}"} if blank
        type.parse(value)
      end

      def primary_key
        opts[:primary_key]
      end

      def primary_key?
        !!primary_key
      end

      def type
        coalesced(opts[:type], Purview::Types::String)
      end

      private

      include Purview::Mixins::Helpers

      attr_reader :opts

      def default_opts
        {}
      end
    end
  end
end

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

    class Boolean < Base
      private

      def default_opts
        super.merge(:type => Purview::Types::Boolean)
      end
    end

    class Date < Base
      private

      def default_opts
        super.merge(:type => Purview::Types::Date)
      end
    end

    class Float < Base
      private

      def default_opts
        super.merge(:type => Purview::Types::Float)
      end
    end

    class Integer < Base
      private

      def default_opts
        super.merge(:type => Purview::Types::Integer)
      end
    end

    class Id < Integer
      private

      def default_opts
        super.merge(:allow_blank => false, :primary_key => true)
      end
    end

    class Money < Base
      private

      def default_opts
        super.merge(:type => Purview::Types::Money)
      end
    end

    class String < Base
      private

      def default_opts
        super.merge(:type => Purview::Types::String, :limit => 255)
      end
    end

    class Text < Base
      private

      def default_opts
        super.merge(:type => Purview::Types::Text)
      end
    end

    class Time < Base
      private

      def default_opts
        super.merge(:type => Purview::Types::Time)
      end
    end

    class Timestamp < Base
      private

      def default_opts
        super.merge(:type => Purview::Types::Timestamp)
      end
    end

    class CreatedTimestamp < Timestamp
      private

      def default_opts
        super.merge(:allow_blank => false)
      end
    end

    class UpdatedTimestamp < Timestamp
      private

      def default_opts
        super.merge(:allow_blank => false)
      end
    end

    class UUID < Base
      private

      def default_opts
        super.merge(:type => Purview::Types::UUID, :limit => 36)
      end
    end
  end
end

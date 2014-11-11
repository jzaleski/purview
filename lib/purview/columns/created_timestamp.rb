module Purview
  module Columns
    class CreatedTimestamp < Timestamp
      private

      def default_opts
        super.merge(:allow_blank => false)
      end
    end
  end
end

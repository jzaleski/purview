module Purview
  module Columns
    class Id < Integer
      private

      def default_opts
        super.merge(:allow_blank => false, :primary_key => true)
      end
    end
  end
end

module FakeS3
  class Server < Sinatra::Base
    include FakeS3::Helpers
    
    db = Mongo::Connection.new.db('s5')
    grid = Mongo::Grid.new(db)

    get '/' do
      'Welcome to FakeS3!'
    end

    get '/:bucket' do
      content_type "application/xml"
      
      headers "Server" => "FakeS3"

      query = { "metadata.bucket" => params[:bucket] }
      limit = (params["max-keys"] || 1000).to_i

      if params[:prefix]
        query[:filename] = Regexp.new(params[:prefix][1..256])
      end

      if params[:marker]
        if params[:prefix]
          query["$where"] = "this.filename > '#{params[:marker]}'"
        else
          query[:filename] = { "$gt" => params[:marker] }
        end
      end

      builder do |x|
        x.instruct! :xml, :version => "1.0", :encoding => "UTF-8"
        x.ListBucketResult :xmlns => "http://s3.amazonaws.com/doc/2006-03-01/" do
          x.Name params[:bucket]
          x.Marker params[:marker]
          db["fs.files"].find(query, :limit => limit, :sort => [:filename, 1]).each do |file|
            x.Contents do
              x.Key file["filename"]
              x.LastModified file["uploadDate"]
              x.ETag file["md5"]
              x.StorageClass "Fake"
              x.Owner do
                x.ID "OWNERID"
                x.DisplayName "someone@example.com"
              end
            end
          end
        end
      end
    end

    head "/:bucket/*" do
      filename = params[:splat].first
      bucket = params[:bucket]
      
      file = grid.get(sha([bucket, filename]))
      headers "Server" => "FakeS3",
              "Date" => Time.now.utc.to_s,
              "Etag" => file["md5"],
              "Last-Modified" => file["uploadDate"].to_s,
              "Age" => (Time.now - file["uploadDate"]).round.to_s,
              "Cache-Control" => "Public",
              "Expires" => (file["uploadDate"] + 315576000).to_s

      content_type file.content_type
    end

    get "/:bucket/*" do
      filename = params[:splat].first
      bucket = params[:bucket]
      headers "Server" => "FakeS3"
      file = grid.get(sha([bucket, filename]))
      content_type file.content_type
      file.read
    end

    put "/:bucket/*" do
      filename = params[:splat].first
      bucket = params[:bucket]
      id = sha([bucket, filename])
      file = request.body

      grid.delete(id)

      grid.put(file.read,
        :_id => id,
        :filename => filename,
        :metadata => {
          :bucket => params[:bucket]
        }
      )
    end
  end
end

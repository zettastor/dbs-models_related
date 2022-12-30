# include thrift-generated code
$:.push('../thrift-generated-src-ruby', __FILE__)

require 'thrift'
require 'information_center'
require 'pp'

begin
  port = ARGV[0]

  transport = Thrift::FramedTransport.new(Thrift::Socket.new('localhost', port))
  protocol = Thrift::BinaryProtocol.new(transport)
  client = InformationCenter::Client.new(protocol)

  transport.open()

  req = ReserveVolumeRequest.new()    
  req.volumeId = ARGV[1].to_i
  req.name = ARGV[2]
  req.volumeSize = ARGV[3].to_i * (2**10)**2
  req.segmentSize = 16 * (2**10)**2
  req.volumeType = ARGV[4].to_i
  req.accountId = ARGV[5].to_i

  pp req
  result = client.reserveVolume(req)
  pp result
  
  transport.close()
rescue
  puts $!
end

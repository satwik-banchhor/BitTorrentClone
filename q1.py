from socket import *
import select
import hashlib
import time

class Invalid_Reception(Exception):
	pass

out = open("q1_big.txt", 'w')
time_out = open("q1_Timing.txt", 'a')

file = bytearray(b'')

def start_connection(serverName, serverPort):
	# Connecting to the server
	clientSocket = socket(AF_INET, SOCK_STREAM)
	clientSocket.connect((serverName,serverPort))

	received_correctly = False

	while not received_correctly:
		# Sending data
		sentence = "GET /big.txt HTTP/1.1\r\nHost: {}\r\n\r\n".format(serverName)
		clientSocket.send(sentence.encode())
		print("Connected to", serverName)

		# Receiving data
		received = b''
		while select.select([clientSocket], [], [], 3)[0]:
			data = clientSocket.recv(2048)
			if not data:
				break
			received += data

		# Updating the downloaded file
		file[:] = received.decode().partition('\r\n\r\n')[2].encode()
		
		# Checking if the received data is correct or not
		md5 = hashlib.md5(file).hexdigest()
		print("MD5 sum of the downloaded file =", md5)
		
		print(file.decode(), file = out, end = '')

		received_correctly = md5 == "70a4b9f4707d258f559f91615297a3ec"

		# If data with same MD5 sum is not received then we connect again and try again
		if not received_correctly:
			print("Invalid Reception, Connecting again")
			clientSocket = socket(AF_INET, SOCK_STREAM)
			clientSocket.connect((serverName,serverPort))
			print("Connected again")

def main():
	# Starting the connection
	start = time.time()
	start_connection("vayu.iitd.ac.in", 80)
	end = time.time()

	# Checking md5 sum of the downloaded file
	md5 = hashlib.md5(file).hexdigest()
	print("MD5 sum matches with the MD5 sum of the file.")

	# Saving the downloaded data into a file
	print(file.decode(), file = out, end = '')

	# Saving time information
	print("\t-- Time:", end - start, file = time_out)

if __name__ == "__main__":
	main()
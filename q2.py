from socket import *
import threading
import select
import hashlib
import csv
import time

class Invalid_Reception(Exception):
	pass

out = open("q2_big.txt", 'w')
time_out = open("q2_Timing.txt", 'a')

file_size = 6488666 # Size of the original file
file = bytearray(('*' * file_size).encode()) # bytearray to store the file
downloaded_file_size = 0 # current size of the file downloaded
chunk_size = 10000 # Chunk Size

def get_chunk():
	# Returns a chuck that has not been downloaded yet
	global downloaded_file_size
	if downloaded_file_size == file_size:
		return ()
	download_from = downloaded_file_size
	downloaded_file_size += chunk_size
	downloaded_file_size = min(downloaded_file_size, file_size)

	return (download_from, downloaded_file_size - 1)

def check(received, expected_size):
	# Checks whether the received data is of valid size or not
	return received.find('\r\n\r\n') != -1 and len(received.partition('\r\n\r\n')[2]) == expected_size

def start_connection(serverName, serverPort):
	# Connecting to the server
	clientSocket = socket(AF_INET, SOCK_STREAM)
	clientSocket.connect((serverName,serverPort))

	print("Connected to", serverName)

	# Getting the initial chunk to download
	chunk = get_chunk()

	receiving_correctly = True # Variable to keep track if the received data is valid or not

	while chunk:
		try:
			# Sending the GET request
			start, end = chunk
			print(start, end)
			sentence = "GET /big.txt HTTP/1.1\r\nHost: {}\r\nConnection: keep-alive\r\nRange: bytes={}-{}\r\n\r\n".format(serverName, start, end)
			clientSocket.send(sentence.encode())

			# Receiving data
			received = b''
			while select.select([clientSocket], [], [], 3)[0]:
				data = clientSocket.recv(2048)
				if not data:
					break
				received += data

			# Checking the received data
			receiving_correctly = check(received.decode(), end - start + 1)
			if not receiving_correctly:
				raise Invalid_Reception

			# If the received data is valid we update the downloaded file bytearray
			file[start : end + 1] = list(map(ord, list(received[-(end - start + 1):].decode())))

		except Exception as e:
			# In case of an invalid reception or some other error we connect again and request the same chunk
			clientSocket = socket(AF_INET, SOCK_STREAM)
			clientSocket.connect((serverName,serverPort))

		# If data is received correctly we request a new chunk
		if receiving_correctly:
			chunk = get_chunk()

def main():
	# Starting the connection
	start = time.time()
	start_connection("vayu.iitd.ac.in", 80)
	end = time.time()

	# Checking md5 sum of the downloaded file
	md5 = hashlib.md5(file).hexdigest()
	print("MD5 sum of the downloaded file =", md5)
	if (md5 == "70a4b9f4707d258f559f91615297a3ec"):
		print("MD5 sum matches with the MD5 sum of the file.")

	# Saving the downloaded data into a file
	print(file.decode(), file = out, end = '')

	# Saving time information
	print("\t-- Time:", end - start, file = time_out)

if __name__ == "__main__":
	main()
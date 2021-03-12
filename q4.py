from socket import *
import threading
import select
import hashlib
import csv
import time
from urllib.parse import urlparse

class Invalid_Reception(Exception):
	pass

out = open("q4_big.txt", 'w')
time_out = open("q4_Timing.txt", 'a')

def take_input():
	global file_size, file
	size_set = False
	# Taking input from the csv file
	with open('Input.csv', newline='') as csvfile:
		reader = list(csv.reader(csvfile, delimiter=',', quotechar='|'))
	for i in range(len(reader)):
		url = urlparse(reader[i][0].strip())
		reader[i][0] = url
		if not size_set:
			clientSocket = socket(AF_INET, SOCK_STREAM)
			clientSocket.settimeout(5)
			clientSocket.connect((url.netloc,80))

			string = "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: keep-alive\r\nRange: bytes={}-{}\r\n\r\n".format(url.path, url.netloc, 0, 1)
			clientSocket.send(string.encode())
			data = clientSocket.recv(4096)

			splt = data.split(b'\r\n\r\n')
			header = splt[0]

			temp = header.split(b"Content-Range: bytes")
			temp = temp[1].split(b"\r\n")[0]
			temp = temp.split(b"/")[1]
			file_size = int(temp)
			file = bytearray(('*' * file_size).encode()) # bytearray to store the file
			print("Setting the size of the object to:", file_size)

			size_set = True

	return reader

file_size = 0 # Size of the original file
file = bytearray(('*' * file_size).encode()) # bytearray to store the file
downloaded_file_size = 0 # current size of the file downloaded
chunk_size = 10000 # Chunk Size

start_time = 0 # Start time

def get_chunk(thread):
	# Returns a chuck that has not been downloaded yet
	global downloaded_file_size
	if downloaded_file_size == file_size:
		return ()
	download_from = downloaded_file_size
	downloaded_file_size += chunk_size
	downloaded_file_size = min(downloaded_file_size, file_size)

	# Updating number of bytes downloaded from the host
	usage[thread_host[thread]] += downloaded_file_size - download_from
	# Updating number of bytes downloaded from the thread
	thread_progress[thread.getName()][0].append(time.time() - start_time)
	thread_progress[thread.getName()][1].append(downloaded_file_size - download_from)
	# Updating the number of bytes downloaded
	times_v_chunks.append([download_from, time.time() - start_time])

	return (download_from, downloaded_file_size - 1)

def check(received, expected_size):
	# Checks whether the received data is of valid size or not
	return received.find('\r\n\r\n') != -1 and len(received.partition('\r\n\r\n')[2]) == expected_size

def start_connection(lock, url):
	# Start a thread
	thread_name = threading.current_thread().getName()

	# Connecting to the server
	connection_attempts = 0
	while (connection_attempts < 10):
		# Trying to reconnect every 5 seconds upto 10 times
		try:
			clientSocket = socket(AF_INET, SOCK_STREAM)
			clientSocket.settimeout(5)
			clientSocket.connect((url.netloc,80))
			break
		except:
			time.sleep(5)
			connection_attempts += 1
			print(thread_name, "Connection Failed, Trying again:", connection_attempts)
	else:
		print("Could not connect")
		return

	print(thread_name, "Connected")

	# Getting the initial chunk to download
	lock.acquire()
	chunk = get_chunk(threading.current_thread())
	print(thread_name, chunk)
	lock.release()

	receiving_correctly = True # Variable to keep track if the received data is valid or not

	while chunk:
		try:
			# Sending the GET request
			start, end = chunk
			print(thread_name, start, end)
			sentence = "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: keep-alive\r\nRange: bytes={}-{}\r\n\r\n".format(url.path, url.netloc, start, end)
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
			print(thread_name, "Connection Failed, Trying again")
			receiving_correctly = False
			connection_attempts = 0
			while (connection_attempts < 10):
				# Trying to reconnect every 5 seconds upto 10 times
				try:
					clientSocket = socket(AF_INET, SOCK_STREAM)
					clientSocket.settimeout(5)
					clientSocket.connect((url.netloc,80))
					break
				except:
					time.sleep(5)
					connection_attempts += 1
					print(thread_name, "Connection Failed, Trying again", connection_attempts)
			else:
				print(thread_name, "Could not connect")
				return
			print(thread_name, "Connected again")

		# If data is received correctly we request a new chunk
		if receiving_correctly:
			lock.acquire()
			chunk = get_chunk(threading.current_thread())
			lock.release()

thread_host = {} # thread-host mapping
usage = {} # host-usage mapping
thread_progress = {}
times_v_chunks = []

def main():
	global start_time, downloaded_file_size

	# Taking input
	input = take_input()

	# Defining lock and threads
	lock = threading.Lock()
	threads = []

	# Starting all threads
	start_time = time.time()
	for i in input:
		usage[i[0].geturl()] = 0
		for j in range(int(i[1].strip())):
			t = threading.Thread(target = start_connection, args=(lock, i[0]))
			threads.append(t)
			thread_host[t] = i[0].geturl() # Mapping thread to host
			thread_progress[t.getName()] = [[], []]
			t.start()

	# join all threads
	for t in threads:
		t.join()

	# Updating the number of bytes downloaded
	times_v_chunks.append([file_size, time.time() - start_time])

	# Checking md5 sum of the downloaded file
	md5 = hashlib.md5(file).hexdigest()
	print("MD5 sum of the downloaded file =", md5)
	if (md5 == "70a4b9f4707d258f559f91615297a3ec"):
		print("MD5 sum matches with the MD5 sum of the file.")

	# Saving the downloaded data into a file
	print(file.decode(), file = out, end = '')

	# Saving time and usage information
	print(input, file = time_out)
	print('\tUsage:', usage, file = time_out)
	print('\tThread Usage:', thread_progress, file = time_out)
	print('\tTime:', times_v_chunks, file = time_out)

if __name__ == "__main__":
	# get_data()
	main()
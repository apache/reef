typedef enum BINARY_TYPE {
    BINARY_TYPE_NONE=0,
	BINARY_TYPE_NATIVE=1,
	BINARY_TYPE_CLR=2,
} BINARY_TYPE ;


BINARY_TYPE IsManagedBinary(const wchar_t* lpszImageName);
